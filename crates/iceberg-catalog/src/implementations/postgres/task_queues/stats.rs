use async_trait::async_trait;
use uuid::Uuid;

use super::{cancel_pending_tasks, TaskFilter};
use crate::api::management::v1::TabularType;
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::tabular::TabularType as DbTabularType;
use crate::implementations::postgres::task_queues::{
    pick_task, queue_task, record_failure, record_success,
};
use crate::service::task_queue::stats::{StatsInput, StatsTask};
use crate::service::task_queue::tabular_purge_queue::{TabularPurgeInput, TabularPurgeTask};
use crate::service::task_queue::{TaskQueue, TaskQueueConfig};

super::impl_pg_task_queue!(StatsQueue);

#[async_trait]
impl TaskQueue for StatsQueue {
    type Task = StatsTask;
    type Input = StatsInput;

    fn config(&self) -> &TaskQueueConfig {
        &self.pg_queue.config
    }

    fn queue_name(&self) -> &'static str {
        "stats"
    }

    #[tracing::instrument(skip(self))]
    async fn pick_new_task(&self) -> crate::api::Result<Option<Self::Task>> {
        let task = pick_task(
            &self.pg_queue.read_write.write_pool,
            self.queue_name(),
            &self.pg_queue.max_age,
        )
        .await?;

        let Some(task) = task else {
            tracing::debug!("No task found in {}", self.queue_name());
            return Ok(None);
        };

        Ok(Some(StatsTask {
            warehouse_ident: task.warehouse_ident,
            task,
        }))
    }

    async fn record_success(&self, id: Uuid) -> crate::api::Result<()> {
        record_success(id, &self.pg_queue.read_write.write_pool).await
    }

    async fn record_failure(&self, id: Uuid, error_details: &str) -> crate::api::Result<()> {
        record_failure(
            &self.pg_queue.read_write.write_pool,
            id,
            self.config().max_retries,
            error_details,
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn enqueue(
        &self,
        StatsInput {
            warehouse_ident,
            parent_id,
        }: StatsInput,
    ) -> crate::api::Result<()> {
        let mut transaction = self
            .pg_queue
            .read_write
            .write_pool
            .begin()
            .await
            .map_err(|e| e.into_error_model("failed begin transaction to purge task"))?;

        tracing::info!("Creating stats task for warehouse: '{warehouse_ident}'",);

        let idempotency_key = warehouse_ident.0;

        let task_id = queue_task(
            &mut transaction,
            self.queue_name(),
            parent_id,
            idempotency_key,
            warehouse_ident,
            None,
        )
        .await?;

        match task_id {
            None => {
                tracing::debug!("Stats task already exists for warehouse: '{warehouse_ident}'",);
            }
            Some(id) => {
                tracing::debug!("Enqueued stats task with id: '{id}'",);
            }
        }

        transaction.commit().await.map_err(|e| {
            tracing::error!(?e, "failed to commit");
            e.into_error_model("failed to commit tabular purge task")
        })?;

        Ok(())
    }

    async fn cancel_pending_tasks(&self, filter: TaskFilter) -> crate::api::Result<()> {
        cancel_pending_tasks(&self.pg_queue, filter, self.queue_name()).await
    }
}

#[cfg(test)]
mod test {
    use super::super::test::setup;
    use crate::service::task_queue::stats::StatsInput;
    use crate::service::task_queue::{TaskQueue, TaskQueueConfig};
    use sqlx::PgPool;

    #[sqlx::test]
    async fn test_queue_stats_task(pool: PgPool) {
        let config = TaskQueueConfig::default();
        let pg_queue = setup(pool, config);
        let queue = super::StatsQueue { pg_queue };
        let input = StatsInput {
            warehouse_ident: uuid::Uuid::new_v4().into(),
            parent_id: None,
        };
        queue.enqueue(input.clone()).await.unwrap();
        queue.enqueue(input.clone()).await.unwrap();

        let task = queue
            .pick_new_task()
            .await
            .unwrap()
            .expect("There should be a task");

        assert_eq!(task.warehouse_ident, input.warehouse_ident);

        let task = queue.pick_new_task().await.unwrap();
        assert!(
            task.is_none(),
            "There should only be one task, idempotency didn't work."
        );
    }
}
