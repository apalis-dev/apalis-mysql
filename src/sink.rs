use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::Sink;
use sqlx::{
    Executor,
    types::chrono::{DateTime, Utc},
};
use ulid::Ulid;

use crate::{MySqlStorage, MySqlTask, error::Error};

/// Push a batch of tasks to the database
pub async fn push_tasks<E>(executor: &mut E, queue: &str, tasks: &[MySqlTask]) -> Result<(), Error>
where
    for<'e> &'e mut E: Executor<'e, Database = sqlx::MySql>,
{
    for task in tasks {
        let id = task
            .task_id()
            .as_ref()
            .map(|id| id.to_string())
            .unwrap_or_else(|| Ulid::generate().to_string());
        let run_at = task.run_at().unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("System time is before Unix epoch")
                .as_secs()
        });
        let run_at = DateTime::from_timestamp(run_at as i64, 0).unwrap_or_else(Utc::now);
        let max_attempts = task.max_attempts().unwrap_or(25) as i32;
        let priority = task.priority().unwrap_or_default() as i32;
        let args = &task.args;
        let idempotency_key = task.idempotency_key();
        let meta = serde_json::to_string(task.metadata()).unwrap_or_default();
        sqlx::query_file!(
            "queries/task/sink.sql",
            args,
            id,
            queue,
            max_attempts,
            run_at,
            priority,
            meta,
            idempotency_key
        )
        .execute(&mut *executor)
        .await?;
    }
    Ok(())
}

impl<Args> Sink<MySqlTask> for MySqlStorage<Args> {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: MySqlTask) -> Result<(), Self::Error> {
        self.project().persistence.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().persistence.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::poll_close(self.project().persistence, cx)
    }
}
