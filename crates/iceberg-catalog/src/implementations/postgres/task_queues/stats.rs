use async_trait::async_trait;
use uuid::Uuid;

use super::{cancel_pending_tasks, TaskFilter};
use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::task_queues::{
    pick_task, queue_task, record_failure, record_success,
};
use crate::service::task_queue::stats::{StatsInput, StatsTask};
use crate::service::task_queue::{Schedule, TaskQueue, TaskQueueConfig};

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
    async fn enqueue(
        &self,
        StatsInput {
            warehouse_ident,
            schedule,
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
            Some(Schedule::Cron { schedule }),
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

    async fn cancel_pending_tasks(&self, filter: TaskFilter) -> crate::api::Result<()> {
        cancel_pending_tasks(&self.pg_queue, filter, self.queue_name()).await
    }
}

#[cfg(test)]
mod test {
    use super::super::test::setup;
    use crate::implementations::postgres::ReadWrite;
    use crate::service::task_queue::stats::StatsInput;
    use crate::service::task_queue::{Scheduler, TaskQueue, TaskQueueConfig};
    use sqlx::PgPool;
    use std::str::FromStr;

    #[sqlx::test]
    async fn test_queue_stats_task(pool: PgPool) {
        let config = TaskQueueConfig::default();
        let rw = ReadWrite::from_pools(pool.clone(), pool.clone());
        let pg_queue = setup(pool.clone(), config);
        let queue = super::StatsQueue { pg_queue };
        let input = StatsInput {
            warehouse_ident: uuid::Uuid::new_v4().into(),
            schedule: cron::Schedule::from_str("*/1 * * * * *").unwrap(),
            parent_id: None,
        };
        queue.enqueue(input.clone()).await.unwrap();
        queue.enqueue(input.clone()).await.unwrap();
        rw.schedule_task_instance().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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
        // after another second there should be a task again
        rw.schedule_task_instance().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let task = queue
            .pick_new_task()
            .await
            .unwrap()
            .expect("There should be a task");
        assert_eq!(task.warehouse_ident, input.warehouse_ident);
    }
}
