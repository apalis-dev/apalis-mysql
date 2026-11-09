use std::{collections::HashSet, str::FromStr, vec};

use apalis_core::{
    backend::{Backend, TaskResult, WaitForCompletion},
    task::{
        status::Status,
        task_id::{TaskId, coalesce_ids},
    },
};
use futures::{StreamExt, stream::BoxStream};
use serde::de::DeserializeOwned;

use crate::{MySqlStorage, error::Error};

#[derive(Debug)]
struct ResultRow {
    pub id: Option<String>,
    pub status: Option<String>,
    pub result: Option<String>,
}

impl<Args, O> WaitForCompletion<O> for MySqlStorage<Args>
where
    Self: Backend<Error = Error>,
    Args: Send,
    O: Send + 'static + DeserializeOwned,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<O>, Self::Error>>;

    fn wait_for(&mut self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        let pool = self.persistence.pool.clone();
        let ids: HashSet<String> = task_ids.into_iter().map(|id| id.to_string()).collect();

        let stream = futures::stream::unfold(ids, move |mut remaining_ids| {
            let pool = pool.clone();
            async move {
                if remaining_ids.is_empty() {
                    return None;
                }

                let ids_vec: Vec<String> = remaining_ids.iter().cloned().collect();
                let ids_vec = coalesce_ids(ids_vec);
                let rows = sqlx::query_file_as!(
                    ResultRow,
                    "queries/backend/fetch_completed_tasks.sql",
                    ids_vec
                )
                .fetch_all(&pool)
                .await
                .ok()?;

                if rows.is_empty() {
                    apalis_core::timer::sleep(std::time::Duration::from_millis(500)).await;
                    return Some((futures::stream::iter(vec![]), remaining_ids));
                }

                let mut results = Vec::new();
                for row in rows {
                    let task_id = row.id.clone().unwrap();
                    remaining_ids.remove(&task_id);
                    let result: Result<O, String> =
                        serde_json::from_str(&row.result.unwrap()).unwrap();
                    results.push(Ok(TaskResult {
                        task_id: TaskId::from_str(&task_id).ok()?,
                        attempt: 0,
                        status: Status::from_str(&row.status.unwrap()).ok()?,
                        result,
                    }));
                }

                Some((futures::stream::iter(results), remaining_ids))
            }
        });
        stream.flatten().boxed()
    }

    async fn check_status(
        &mut self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> Result<Vec<TaskResult<O>>, Self::Error> {
        let pool = self.persistence.pool.clone();
        let ids: Vec<String> = task_ids.into_iter().map(|id| id.to_string()).collect();
        let ids = serde_json::to_string(&ids).map_err(Error::JsonError)?;

        let rows =
            sqlx::query_file_as!(ResultRow, "queries/backend/fetch_completed_tasks.sql", ids)
                .fetch_all(&pool)
                .await?;

        let mut results = Vec::new();
        for row in rows {
            let task_id = TaskId::from_str(&row.id.unwrap())
                .map_err(|_| sqlx::Error::Decode("Invalid task ID".into()))?;

            let result: Result<O, String> = serde_json::from_str(&row.result.unwrap())
                .map_err(|_| sqlx::Error::Decode("Failed to decode result".into()))?;

            results.push(TaskResult {
                task_id,
                attempt: 0,
                status: row
                    .status
                    .unwrap()
                    .parse()
                    .map_err(|_| sqlx::Error::Decode("Invalid status value".into()))?,
                result,
            });
        }

        Ok(results)
    }
}
