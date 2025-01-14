// TODO: remove, this approach leads to reimplementing a second layer of task queues, i.e. the
//       scheduler would have to either scan all stored schedules periodically which is bad for
//       obvious obvious reasons, the alternative is to store a schedule + next-execution date and
//       poll it via for update skip locked which is just what the task queue does.

// use crate::service::task_queue::TaskQueues;
// use crate::service::TableIdentUuid;
// use anyhow::Result;
// use uuid::Uuid;
//
// pub struct JobId(Uuid);
//
// pub struct Job {
//     id: JobId,
//     definition: JobDefinition,
//     status: JobStatus,
//     created_at: chrono::DateTime<chrono::Utc>,
//     updated_at: chrono::DateTime<chrono::Utc>,
// }
//
// pub struct JobDefinition {
//     next_invocation: chrono::DateTime<chrono::Utc>,
//     schedule: cron::Schedule,
//     job_type: JobType,
// }
//
// pub enum JobStatus {
//     Active,
//     Inactive,
// }
//
// pub enum JobType {
//     Compaction { table_ident_uuid: TableIdentUuid },
//     Stats {},
// }
//
// pub struct JobFilter {
//     status: Option<JobStatus>,
//     ids: Option<Vec<JobId>>,
//     typ: Option<Vec<JobType>>,
// }
//
// pub trait Scheduler {
//     /// Creates a schedule in the backing store.
//     async fn schedule(&self, job: Job, idempotency_key: Uuid) -> Result<Job>;
//     /// Deactivates a job
//     async fn deactivate(&self, job_id: JobId) -> Result<()>;
//     /// Check the status of a single job.
//     async fn status(&self, job_id: JobId) -> Result<JobStatus> {
//         self.list(JobFilter {
//             status: None,
//             ids: Some(vec![job_id]),
//             typ: None,
//         })
//     }
//     /// Scans cron-storage for jobs that are due to run.
//     async fn scan(&self, limit: usize) -> Result<Vec<Job>>;
//     /// Lists all jobs in the scheduler.
//     async fn list(&self, filter: JobFilter) -> Result<Vec<Job>>;
// }
//
// pub fn scheduler_task(scheduler: impl Scheduler, queues: TaskQueues) {
//     tokio::spawn(async move {
//         loop {
//             let jobs = scheduler.scan(100).await.unwrap();
//             for job in jobs {
//                 match job.definition.job_type {
//                     JobType::Compaction { .. } => {}
//                     JobType::Stats {} => {
//                         // queues
//                         //     .stats_queue
//                         //     .push(job.id, job.definition.next_invocation)
//                         //     .await
//                         //     .unwrap();
//                     }
//                 }
//             }
//             tokio::time::sleep(std::time::Duration::from_secs(60)).await;
//         }
//     });
// }
