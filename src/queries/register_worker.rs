use apalis_core::worker::context::WorkerContext;
use sqlx::Executor;

use crate::{config::Config, error::Error};

/// Register a new worker in the database
pub async fn register_worker<E>(
    executor: &mut E,
    config: &Config,
    worker: &WorkerContext,
    storage_type: &str,
) -> Result<(), Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::MySql>,
{
    let worker_id = worker.name().to_owned();
    let queue = config.queue.as_ref();
    let layers = worker.get_service().to_owned();
    let keep_alive = config.heartbeat_interval.as_secs();
    let res = sqlx::query_file!(
        "queries/backend/register_worker.sql",
        worker_id,
        queue,
        storage_type,
        layers,
        worker_id,
        keep_alive,
    )
    .execute(&mut *executor)
    .await?;
    if res.rows_affected() == 0 {
        return Err(Error::WorkerAlreadyExists(worker.name().to_owned()));
    }
    Ok(())
}
