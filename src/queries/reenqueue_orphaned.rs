use apalis_core::task::task_id::coalesce_ids;
use sqlx::Executor;

use crate::error::Error;

/// Re-enqueue tasks that were being processed by dead workers
///
/// A worker that has not sent a keep-alive signal within the heartbeat duration is considered dead
pub async fn reenqueue_orphaned<E>(
    executor: &mut E,
    queue: &str,
    dead_for: u64,
) -> Result<u64, Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::MySql>,
{
    let dead_for = -(dead_for as i64);
    match sqlx::query_file!("queries/backend/reenqueue_orphaned.sql", dead_for, queue)
        .execute(executor)
        .await
    {
        Ok(res) => {
            if res.rows_affected() > 0 {
                log::info!(
                    "Re-enqueued {} orphaned tasks that were being processed by dead workers",
                    res.rows_affected()
                );
            }
            Ok(res.rows_affected())
        }
        Err(e) => {
            log::error!("Failed to re-enqueue orphaned tasks: {e}");
            Err(e.into())
        }
    }
}

/// Re-enqueue tasks that were being processed by a dying worker
///
/// This is invoked during `Backend::poll_close`
pub async fn reenqueue_abandoned<E>(
    executor: &mut E,
    queue: &str,
    worker: &str,
    task_ids: &[String],
) -> Result<u64, Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::MySql>,
{
    // `coalesce_ids` already returns a JSON array; serializing it again would
    // produce a JSON string and `JSON_TABLE` would match no rows.
    let task_ids = coalesce_ids(task_ids);
    let res = sqlx::query_file!(
        "queries/backend/reenqueue_abandoned.sql",
        queue,
        worker,
        task_ids
    )
    .execute(executor)
    .await?;
    Ok(res.rows_affected())
}
