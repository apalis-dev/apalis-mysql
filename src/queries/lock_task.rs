use sqlx::Executor;

use crate::error::Error;

/// Lock multiple tasks, given a worker
pub async fn lock_tasks<E>(
    executor: &mut E,
    task_ids: &[String],
    worker_id: &str,
) -> Result<u64, Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::MySql>,
{
    let task_ids = serde_json::to_string(task_ids).map_err(Error::JsonError)?;
    let res = sqlx::query_file!("queries/task/lock_by_id.sql", worker_id, task_ids)
        .execute(executor)
        .await?;
    Ok(res.rows_affected())
}
