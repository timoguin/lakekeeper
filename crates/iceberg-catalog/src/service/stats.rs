#![allow(clippy::module_name_repetitions)]
use itertools::{FoldWhile, Itertools};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

#[async_trait::async_trait]
pub trait StatsExt: Send + Sync + 'static {
    async fn stats(&self) -> Vec<Stats>;
    async fn update_stats(&self);
    async fn update_stats_task(
        self: Arc<Self>,
        refresh_interval: Duration,
        jitter_millis: u64,
    ) -> JoinHandle<()> {
        tokio::task::spawn(async move {
            loop {
                self.update_stats().await;
                let jitter = { rand::thread_rng().next_u64().min(jitter_millis) };
                tokio::time::sleep(refresh_interval + Duration::from_millis(jitter)).await;
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, strum::Display, Deserialize, Serialize)]
pub enum Stat {
    Scalar { name: String, value: usize },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Stats {
    name: String,
    #[serde(with = "chrono::serde::ts_milliseconds", rename = "lastCheck")]
    checked_at: chrono::DateTime<chrono::Utc>,
    stats: Vec<Stat>,
}

impl Stats {
    #[must_use]
    pub fn now(name: &'static str, stats: Vec<Stat>) -> Self {
        Self {
            name: name.into(),
            checked_at: chrono::Utc::now(),
            stats,
        }
    }

    #[must_use]
    pub fn stats(&self) -> &[Stat] {
        &self.stats
    }
}

#[derive(Clone)]
pub struct ServiceStatsProvider {
    providers: Vec<(&'static str, Arc<dyn StatsExt + Sync + Send>)>,
    check_jitter_millis: u64,
    check_frequency_seconds: u64,
}

impl std::fmt::Debug for ServiceStatsProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceStatsProvider")
            .field(
                "providers",
                &self
                    .providers
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>(),
            )
            .field("check_jitter_millis", &self.check_jitter_millis)
            .field("check_frequency_seconds", &self.check_frequency_seconds)
            .finish()
    }
}

impl ServiceStatsProvider {
    #[must_use]
    pub fn new(
        providers: Vec<(&'static str, Arc<dyn StatsExt + Sync + Send>)>,
        check_frequency_seconds: u64,
        check_jitter_millis: u64,
    ) -> Self {
        Self {
            providers,
            check_jitter_millis,
            check_frequency_seconds,
        }
    }

    pub async fn spawn_stats_collectors(&self) {
        for (service_name, provider) in &self.providers {
            let provider = provider.clone();
            provider
                .update_stats_task(
                    Duration::from_secs(self.check_frequency_seconds),
                    self.check_jitter_millis,
                )
                .await;
            tracing::info!("Spawned stats provider: {service_name}");
        }
    }

    pub async fn collect_health(&self) -> HealthState {
        let mut services = HashMap::new();
        for (name, provider) in &self.providers {
            let provider_health = provider.stats().await;
            services.insert((*name).to_string(), provider_health);
        }

        HealthState { services }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthState {
    pub services: HashMap<String, Vec<Stats>>,
}
