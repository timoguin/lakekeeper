use crate::service::task_queue::{TaskInstance, TaskQueue};
use crate::service::{Catalog, ListFlags};
use crate::WarehouseIdent;
use std::sync::Arc;

use std::time::Duration;
use tracing::Instrument;
use uuid::Uuid;

pub type StatsQueue =
    Arc<dyn TaskQueue<Task = StatsTask, Input = StatsInput> + Send + Sync + 'static>;

// TODO: concurrent workers
pub async fn stats_task<C: Catalog>(fetcher: StatsQueue, catalog_state: C::State) {
    loop {
        // add some jitter to avoid syncing with other queues
        // TODO: probably should have a random number here
        tokio::time::sleep(fetcher.config().poll_interval + Duration::from_millis(13)).await;

        let stats_task = match fetcher.pick_new_task().await {
            Ok(expiration) => expiration,
            Err(err) => {
                // TODO: add retry counter + exponential backoff
                tracing::error!("Failed to fetch stats task: {:?}", err);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let Some(purge_task) = stats_task else {
            continue;
        };

        let span = tracing::debug_span!(
            "statistics",
            warehouse_id = %purge_task.warehouse_ident,
            queue_name = %purge_task.task.queue_name,
            task = ?purge_task.task,
        );

        instrumented_collect_stats::<C>(fetcher.clone(), catalog_state.clone(), &purge_task)
            .instrument(span.or_current())
            .await;
    }
}

async fn instrumented_collect_stats<C: Catalog>(
    fetcher: Arc<dyn TaskQueue<Task = StatsTask, Input = StatsInput> + Send + Sync>,
    catalog_state: C::State,
    purge_task: &StatsTask,
) {
    match C::update_warehouse_statistics(
        purge_task.warehouse_ident,
        ListFlags {
            include_active: true,
            include_staged: false,
            include_deleted: false,
        },
        catalog_state,
    )
    .await
    {
        Ok(stats) => {
            fetcher.retrying_record_success(&purge_task.task).await;
            tracing::info!(
                "Successfully collected stats for warehouse '{}', tables: '{}', views: '{}'",
                purge_task.warehouse_ident,
                stats.number_of_tables,
                stats.number_of_views
            );
        }
        Err(err) => {
            tracing::error!(
                "Failed to collect stats for warehouse '{}' due to: '{:?}'",
                purge_task.warehouse_ident,
                err.error
            );
            fetcher
                .retrying_record_failure(&purge_task.task, &err.error.to_string())
                .await;
        }
    };
}

#[derive(Debug)]
pub struct StatsTask {
    pub warehouse_ident: WarehouseIdent,
    pub task: TaskInstance,
}

#[derive(Debug, Clone)]
pub struct StatsInput {
    pub warehouse_ident: WarehouseIdent,
    pub schedule: cron::Schedule,
    pub parent_id: Option<Uuid>,
}
