use apalis_core::worker::context::WorkerContext;
use sqlx::Executor;

use crate::{config::Config, error::Error};

/// Send a keep-alive signal to the database to indicate that the worker is still active
pub async fn keep_alive<E>(
    executor: &mut E,
    config: &Config,
    worker: &WorkerContext,
) -> Result<(), Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::MySql>,
{
    let worker = worker.name().to_owned();
    let queue = config.queue.as_ref();
    let res = sqlx::query_file!("queries/backend/keep_alive.sql", worker, queue)
        .execute(executor)
        .await?;
    if res.rows_affected() == 0 {
        return Err(Error::WorkerOutOfSync);
    }
    Ok(())
}
