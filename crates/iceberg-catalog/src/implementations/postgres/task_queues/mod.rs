mod tabular_expiration_queue;
mod tabular_purge_queue;

use crate::implementations::postgres::dbutils::DBErrorHandler;
use crate::implementations::postgres::ReadWrite;
use crate::service::task_queue::{
    Schedule, Scheduler, Task, TaskFilter, TaskInstance, TaskQueueConfig, TaskStatus,
};
use crate::WarehouseIdent;
use anyhow::Error;
use async_trait::async_trait;
use std::str::FromStr;
pub use tabular_expiration_queue::TabularExpirationQueue;
pub use tabular_purge_queue::TabularPurgeQueue;

use chrono::{DateTime, Utc};
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use sqlx::{Acquire, PgConnection, PgPool};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PgQueue {
    pub read_write: ReadWrite,
    pub config: TaskQueueConfig,
    pub max_age: sqlx::postgres::types::PgInterval,
}

impl PgQueue {
    pub fn new(read_write: ReadWrite) -> Self {
        let config = TaskQueueConfig::default();
        let microseconds = config
            .max_age
            .num_microseconds()
            .expect("Invalid max age duration for task queues hard-coded in Default.");
        Self {
            read_write,
            config,
            max_age: sqlx::postgres::types::PgInterval {
                months: 0,
                days: 0,
                microseconds,
            },
        }
    }

    fn from_config(read_write: ReadWrite, config: TaskQueueConfig) -> anyhow::Result<Self> {
        let microseconds = config
            .max_age
            .num_microseconds()
            .ok_or(anyhow::anyhow!("Invalid max age duration for task queues."))?;
        Ok(Self {
            read_write,
            config,
            max_age: sqlx::postgres::types::PgInterval {
                months: 0,
                days: 0,
                microseconds,
            },
        })
    }
}

#[async_trait]
impl Scheduler for PgQueue {
    async fn schedule_task_instance(&self) -> Result<(), IcebergErrorResponse> {
        let mut conn = self.read_write.write_pool.acquire().await.map_err(|e| {
            e.into_error_model("Failed to acquire connection to schedule task instance")
        })?;
        schedule_task(&mut conn, None).await
    }
}

pub struct TaskSchedule {
    pub task_id: Uuid,
    pub schedule: DateTime<Utc>,
}

async fn schedule(
    conn: &mut PgConnection,
    task_id: Uuid,
    idempotency_key: Uuid,
    schedule: Schedule,
) -> Result<(), IcebergErrorResponse> {
    let (next_tick, run_at, idempotency_key_data) = match schedule {
        Schedule::RunAt(dt) => (None, Some(dt), Some(dt.to_rfc3339())),
        Schedule::Cron(cron) => {
            let next = cron.upcoming(Utc).next();
            (
                Some(Utc::now()),
                cron.upcoming(Utc).next(),
                next.map(|dt| dt.to_rfc3339()),
            )
        }
        Schedule::Immediate => (None, Some(Utc::now()), None),
    };

    let idempotency_key = if let Some(idempotency_key_data) = idempotency_key_data {
        Uuid::new_v5(&idempotency_key, idempotency_key_data.as_bytes())
    } else {
        idempotency_key
    };

    let has_next = next_tick.is_some();
    // TODO: Updating task status like this transitions the task to done before its last
    //       instance is done. That means we'll probably have to introduce a secondary status
    //       to capture the success-state of the task as a function of its instances.
    sqlx::query!(
            r#"
                WITH updated_tasks AS (UPDATE task
                    SET next_tick = $5,
                    status = CASE WHEN $6 THEN 'active'::task_status2 ELSE 'done'::task_status2 END
                    WHERE task_id = $1)
                INSERT INTO task_instance (task_id, task_instance_id, status, suspend_until, idempotency_key)
                    VALUES
                    ($1, $2, 'pending', $3, $4)
                ON CONFLICT ON CONSTRAINT task_instance_unique_idempotency_key
                DO UPDATE SET
                    status = EXCLUDED.status,
                    suspend_until = EXCLUDED.suspend_until
                WHERE task_instance.status = 'cancelled'
                "#,
            task_id,
            Uuid::now_v7(),
            run_at,
            idempotency_key,
            next_tick,
            has_next
        )
        .execute(conn)
        .await
        .map_err(|e| e.into_error_model("Failed to schedule task instance"))?;
    eprintln!("scheduled.");

    Ok(())
}

async fn schedule_task(
    read_write: &mut PgConnection,
    single_task: Option<Uuid>,
) -> Result<(), IcebergErrorResponse> {
    // TODO: should we schedule more than one at a time?
    let task = sqlx::query!(
            r#"
            SELECT t.task_id, schedule, t.idempotency_key
            FROM task t
            WHERE (($1 = t.task_id or $1 is null) AND t.status = 'active' AND ((next_tick < now() AT TIME ZONE 'UTC')))
            FOR UPDATE SKIP LOCKED
            LIMIT 1"#,
            single_task
        ).fetch_optional(&mut *read_write).await.map_err(|e| e.into_error_model("Failed to begin transaction"))?;

    eprintln!("Found {task:?}");
    if let Some(row) = task {
        let task_id = row.task_id;
        let sched = row
            .schedule
            .as_deref()
            .map(|s| cron::Schedule::from_str(&s))
            .transpose()
            .map_err(|e| {
                ErrorModel::internal(
                    "Failed to parse cron schedule from database.",
                    "InternalDatabaseError",
                    Some(Box::new(e)),
                )
            })?
            .map(Schedule::Cron);

        schedule(
            read_write,
            task_id,
            row.idempotency_key,
            sched.unwrap_or(Schedule::Immediate),
        )
        .await?;
    }
    Ok(())
}

async fn queue_task(
    conn: &mut PgConnection,
    queue_name: &str,
    parent_task_id: Option<Uuid>,
    idempotency_key: Uuid,
    warehouse_ident: WarehouseIdent,
    schedul: Option<Schedule>,
) -> Result<Option<Uuid>, IcebergErrorResponse> {
    // let (next_tick, schedule) = match schedule {
    //     Some(Schedule::RunAt(dt)) => (Some(Utc::now()), None),
    //     Some(Schedule::Cron(cron)) => (Some(Utc::now()), Some(cron.to_string())),
    //     Some(Schedule::Immediate) => (Some(Utc::now()), None),
    //     None => (Some(Utc::now()), None),
    // };
    let sched = schedul
        .as_ref()
        .map(|s| match s {
            Schedule::Cron(sched) => Some(sched.to_string()),
            _ => None,
        })
        .flatten();

    let task_id = Uuid::now_v7();
    let inserted = sqlx::query_scalar!(
        r#"INSERT INTO task(
                task_id,
                queue_name,
                status,
                parent_task_id,
                idempotency_key,
                warehouse_id,
                schedule,
                version)
        VALUES ($1, $2, 'active', $3, $4, $5, $6, $7)
        ON CONFLICT ON CONSTRAINT unique_idempotency_key
        DO NOTHING
        RETURNING task_id"#,
        task_id,
        queue_name,
        parent_task_id,
        idempotency_key,
        *warehouse_ident,
        sched,
        0, // TODO: add version,
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(|e| e.into_error_model("failed queueing task"))?;
    if let Some(inserted) = inserted {
        schedule(
            conn,
            inserted,
            idempotency_key,
            schedul.unwrap_or(Schedule::Immediate),
        )
        .await?;
    }
    Ok(inserted)
}

async fn record_failure(
    conn: &PgPool,
    id: Uuid,
    n_retries: i32,
    details: &str,
) -> Result<(), IcebergErrorResponse> {
    let r = sqlx::query!(
        r#"
        WITH cte as (
            SELECT attempt >= $2 as should_fail
            FROM task_instance
            WHERE task_instance_id = $1
        )
        UPDATE task_instance
        SET status = CASE WHEN (select should_fail from cte) THEN 'failed'::task_status ELSE 'pending'::task_status END,
            last_error_details = $3
        WHERE task_instance_id = $1
        RETURNING task_instance_id, (select should_fail from cte) as "should_fail!"
        "#,
        id,
        n_retries,
        details
    )
        .fetch_one(conn)
        .await.map_err(|e| e.into_error_model("failed to record task failure"))?;
    eprintln!("{r:?}");
    Ok(())
}

#[tracing::instrument]
async fn pick_task(
    pool: &PgPool,
    queue_name: &'static str,
    max_age: &sqlx::postgres::types::PgInterval,
) -> Result<Option<TaskInstance>, IcebergErrorResponse> {
    let x = sqlx::query_as!(
        TaskInstance,
        r#"
        WITH updated_task AS (
            SELECT ti.task_id, ti.task_instance_id, t.queue_name, t.parent_task_id
            FROM task_instance ti JOIN task t ON ti.task_id = t.task_id
            WHERE (ti.status = 'pending' AND t.queue_name = $1 AND ((ti.suspend_until < now() AT TIME ZONE 'UTC') OR (ti.suspend_until IS NULL)))
                    OR (ti.status = 'running' AND (now() - ti.picked_up_at) > $2)
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE task_instance ti
        SET status = 'running', picked_up_at = now(), attempt = ti.attempt + 1
        FROM updated_task
        WHERE ti.task_instance_id = updated_task.task_instance_id
        RETURNING ti.task_id, ti.task_instance_id, ti.status as "status: TaskStatus", ti.picked_up_at, ti.attempt, (select parent_task_id from updated_task), (select queue_name from updated_task) as "queue_name!"
        "#,
        queue_name,
        max_age,
    )
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            tracing::error!(?e, "Failed to pick a task");
            e.into_error_model(format!("Failed to pick a '{queue_name}' task"))
        })?;

    if let Some(task) = x.as_ref() {
        tracing::info!("Picked up task: {:?}", task);
    }

    Ok(x)
}

async fn record_success(id: Uuid, pool: &PgPool) -> Result<(), IcebergErrorResponse> {
    let _ = sqlx::query!(
        r#"
        UPDATE task_instance
        SET status = 'done'
        WHERE task_instance_id = $1
        "#,
        id
    )
    .execute(pool)
    .await
    .map_err(|e| e.into_error_model("failed to record task success"))?;
    Ok(())
}

macro_rules! impl_pg_task_queue {
    ($name:ident) => {
        use crate::implementations::postgres::task_queues::PgQueue;
        use crate::implementations::postgres::ReadWrite;

        #[derive(Debug, Clone)]
        pub struct $name {
            pg_queue: PgQueue,
        }

        impl $name {
            #[must_use]
            pub fn new(read_write: ReadWrite) -> Self {
                Self {
                    pg_queue: PgQueue::new(read_write),
                }
            }

            /// Create a new `$name` with the default configuration.
            ///
            /// # Errors
            /// Returns an error if the max age duration is invalid.
            pub fn from_config(
                read_write: ReadWrite,
                config: TaskQueueConfig,
            ) -> anyhow::Result<Self> {
                Ok(Self {
                    pg_queue: PgQueue::from_config(read_write, config)?,
                })
            }
        }
    };
}
use impl_pg_task_queue;

/// Cancel pending tasks for a warehouse
/// If `task_ids` are provided in `filter` which are not pending, they are ignored
async fn cancel_pending_tasks(
    queue: &PgQueue,
    filter: TaskFilter,
    queue_name: &'static str,
) -> crate::api::Result<()> {
    let mut transaction = queue
        .read_write
        .write_pool
        .begin()
        .await
        .map_err(|e| e.into_error_model("Failed to get transaction to cancel Task"))?;
    // TODO: we're only cancelling task_instances here, have to cancel tasks elsewhere too, probably different api?
    match filter {
        TaskFilter::WarehouseId(warehouse_id) => {
            sqlx::query!(
                r#"
                    UPDATE task_instance ti SET status = 'cancelled'
                    FROM task
                    WHERE ti.status = 'pending'
                    AND task.warehouse_id = $1
                    AND task.queue_name = $2
                "#,
                *warehouse_id,
                queue_name
            )
            .fetch_all(&mut *transaction)
            .await
            .map_err(|e| {
                tracing::error!(
                    ?e,
                    "Failed to cancel {queue_name} Tasks for warehouse {warehouse_id}"
                );
                e.into_error_model(format!(
                    "Failed to cancel {queue_name} Tasks for warehouse {warehouse_id}"
                ))
            })?;
        }
        TaskFilter::TaskIds(task_ids) => {
            sqlx::query!(
                r#"
                    UPDATE task_instance SET status = 'cancelled'
                    WHERE status = 'pending'
                    AND task_instance_id = ANY($1)
                "#,
                &task_ids.iter().map(|s| **s).collect::<Vec<_>>(),
            )
            .fetch_all(&mut *transaction)
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to cancel Tasks for task_ids {task_ids:?}");
                e.into_error_model("Failed to cancel Tasks for specified ids")
            })?;
        }
    }

    transaction.commit().await.map_err(|e| {
        tracing::error!(?e, "Failed to commit transaction to cancel Tasks");
        e.into_error_model("failed to commit transaction cancelling tasks.")
    })?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::WarehouseIdent;
    use sqlx::PgPool;
    use uuid::Uuid;
    const TEST_WAREHOUSE: WarehouseIdent = WarehouseIdent(Uuid::nil());

    #[sqlx::test]
    async fn test_queue_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();

        let idempotency_key = Uuid::new_v5(&TEST_WAREHOUSE, b"test");

        let id = queue_task(
            &mut conn,
            "test",
            None,
            idempotency_key,
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap();

        assert!(queue_task(
            &mut conn,
            "test",
            None,
            idempotency_key,
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .is_none());

        let id3 = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test2"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap();

        assert_ne!(id, id3);
    }

    pub(crate) fn setup(pool: PgPool, config: TaskQueueConfig) -> PgQueue {
        PgQueue::from_config(ReadWrite::from_pools(pool.clone(), pool), config).unwrap()
    }

    #[sqlx::test]
    async fn test_failed_tasks_are_put_back(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);
        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        schedule_task(&mut conn, None).await.unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_failure(&pool, task.task_instance_id, 5, "test")
            .await
            .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_failure(&pool, task.task_instance_id, 2, "test")
            .await
            .unwrap();

        assert_eq!(
            pick_task(&pool, "test", &queue.max_age).await.unwrap(),
            None
        );
    }

    #[sqlx::test]
    async fn test_success_task_arent_polled(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        schedule_task(&mut conn, None).await.unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_success(id, &pool).await.unwrap();

        assert!(pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .is_none());
    }

    #[sqlx::test]
    async fn test_scheduled_tasks_are_polled_later(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            TEST_WAREHOUSE,
            Some(Schedule::RunAt(
                Utc::now() + chrono::Duration::milliseconds(500),
            )),
        )
        .await
        .unwrap()
        .unwrap();
        schedule_task(&mut conn, None).await.unwrap();

        assert_eq!(
            pick_task(&pool, "test", &queue.max_age).await.unwrap(),
            None
        );

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        schedule_task(&mut conn, None).await.unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
    }

    #[sqlx::test]
    async fn test_stale_tasks_are_picked_up_again(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig {
            max_age: chrono::Duration::milliseconds(500),
            ..Default::default()
        };
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
    }

    #[sqlx::test]
    async fn test_multiple_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let queue = setup(pool.clone(), config);

        let id = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        schedule_task(&mut conn, None).await.unwrap();

        let id2 = queue_task(
            &mut conn,
            "test",
            None,
            Uuid::new_v5(&TEST_WAREHOUSE, b"test2"),
            TEST_WAREHOUSE,
            None,
        )
        .await
        .unwrap()
        .unwrap();
        schedule_task(&mut conn, None).await.unwrap();

        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();
        schedule_task(&mut conn, None).await.unwrap();

        assert!(
            pick_task(&pool, "test", &queue.max_age)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, "test");

        record_success(task.task_id, &pool).await.unwrap();
        record_success(id2, &pool).await.unwrap();
    }
}
