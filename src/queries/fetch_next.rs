use apalis_core::{task::status::Status, worker::context::WorkerContext};
use sqlx::{Executor, types::chrono::Utc};

use crate::{MySqlTask, config::Config, error::Error, from_row::MySqlTaskRow};

/// Fetch the next batch of tasks from the mysql backend
pub async fn fetch_next<E>(
    executor: &mut E,
    config: &Config,
    worker: &WorkerContext,
) -> Result<Vec<MySqlTask>, Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::MySql>,
{
    let job_type = config.queue.as_ref();
    let buffer_size = config.batch_size as i32;
    let run_at = Utc::now();

    let rows = sqlx::query_file_as!(
        MySqlTaskRow,
        "queries/backend/fetch_next.sql",
        job_type,
        run_at,
        buffer_size
    )
    .fetch_all(&mut *executor)
    .await?;

    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let ids = rows
        .iter()
        .filter_map(|row| row.id.clone())
        .collect::<Vec<_>>();
    let ids = serde_json::to_string(&ids).map_err(Error::JsonError)?;
    sqlx::query_file!("queries/backend/claim_tasks.sql", worker.name(), ids)
        .execute(&mut *executor)
        .await?;

    rows.into_iter()
        .map(|row| row.into_task(Some(Status::Queued)))
        .collect()
}
