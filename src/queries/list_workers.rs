use apalis_core::backend::{Backend, ListWorkers, RunningWorker};
use futures::TryFutureExt;

use sqlx::types::chrono::{DateTime, Utc};

use crate::{MySqlStorage, error::Error, timestamp::Timestamp};

struct Worker {
    id: String,
    worker_type: String,
    storage_name: String,
    layers: Option<String>,
    last_seen: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
}

impl<Args: Sync> ListWorkers for MySqlStorage<Args>
where
    Self: Backend<Error = Error>,
{
    fn list_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let queue = self.persistence.config.queue.to_string();
        let pool = self.persistence.pool.clone();
        let limit = 100;
        let offset = 0;
        async move {
            let workers = sqlx::query_file_as!(
                Worker,
                "queries/backend/list_workers.sql",
                queue,
                limit,
                offset
            )
            .fetch_all(&pool)
            .map_ok(|w| {
                w.into_iter()
                    .map(|w| RunningWorker {
                        id: w.id,
                        backend: w.storage_name,
                        started_at: w
                            .started_at
                            .map(Timestamp::from)
                            .map(|t| t.0)
                            .unwrap_or_default(),
                        last_heartbeat: w
                            .last_seen
                            .map(Timestamp::from)
                            .map(|t| t.0)
                            .unwrap_or_default(),
                        layers: w.layers.unwrap_or_default(),
                        queue: w.worker_type,
                    })
                    .collect()
            })
            .await?;
            Ok(workers)
        }
    }

    fn list_all_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();
        let limit = 100;
        let offset = 0;
        async move {
            let workers = sqlx::query_file_as!(
                Worker,
                "queries/backend/list_all_workers.sql",
                limit,
                offset
            )
            .fetch_all(&pool)
            .map_ok(|w| {
                w.into_iter()
                    .map(|w| RunningWorker {
                        id: w.id,
                        backend: w.storage_name,
                        started_at: w
                            .started_at
                            .map(Timestamp::from)
                            .map(|t| t.0)
                            .unwrap_or_default(),
                        last_heartbeat: w
                            .last_seen
                            .map(Timestamp::from)
                            .map(|t| t.0)
                            .unwrap_or_default(),
                        layers: w.layers.unwrap_or_default(),
                        queue: w.worker_type,
                    })
                    .collect()
            })
            .await?;
            Ok(workers)
        }
    }
}
