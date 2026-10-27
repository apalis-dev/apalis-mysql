use apalis_core::backend::{Backend, ListQueues, QueueInfo};

use crate::{MySqlStorage, error::Error};

struct QueueInfoRow {
    name: Option<String>,
    stats: Option<serde_json::Value>,
    workers: Option<serde_json::Value>,
    activity: Option<serde_json::Value>,
}

impl From<QueueInfoRow> for QueueInfo {
    fn from(row: QueueInfoRow) -> Self {
        Self {
            name: row.name.unwrap_or_default(),
            stats: row
                .stats
                .and_then(|s| serde_json::from_value(s).ok())
                .unwrap_or_default(),
            workers: row
                .workers
                .and_then(|s| serde_json::from_value(s).ok())
                .unwrap_or_default(),
            activity: row
                .activity
                .and_then(|s| serde_json::from_value(s).ok())
                .unwrap_or_default(),
        }
    }
}

impl<Args> ListQueues for MySqlStorage<Args>
where
    Self: Backend<Error = Error>,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let pool = self.persistence.pool.clone();

        async move {
            let queues = sqlx::query_file_as!(QueueInfoRow, "queries/backend/list_queues.sql")
                .fetch_all(&pool)
                .await?
                .into_iter()
                .map(QueueInfo::from)
                .collect();
            Ok(queues)
        }
    }
}
