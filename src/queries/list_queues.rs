use apalis_core::backend::{BackendExt, ListQueues, QueueInfo};
use ulid::Ulid;

use crate::{CompactType, MySqlContext, MySqlStorage};

struct QueueInfoRow {
    name: Option<String>,
    stats: serde_json::Value,
    workers: serde_json::Value,
    activity: serde_json::Value,
}

impl From<QueueInfoRow> for QueueInfo {
    fn from(row: QueueInfoRow) -> Self {
        Self {
            name: row.name.unwrap_or_default(),
            stats: serde_json::from_value(row.stats).unwrap(),
            workers: serde_json::from_value(row.workers).unwrap(),
            activity: serde_json::from_value(row.activity).unwrap(),
        }
    }
}

impl<Args, D, F> ListQueues for MySqlStorage<Args, D, F>
where
    Self: BackendExt<
            Context = MySqlContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = sqlx::Error,
        >,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let pool = self.pool.clone();

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
