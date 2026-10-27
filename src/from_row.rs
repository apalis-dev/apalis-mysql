use std::{collections::HashMap, str::FromStr};

use apalis_core::task::{
    builder::TaskBuilder,
    metadata::MetadataStore,
    status::Status,
    task_id::{TaskId, TaskIdError::Decode},
};
use sqlx::types::chrono::{DateTime, Utc};
use ulid::Ulid;

use crate::{MySqlTask, error::Error, timestamp::Timestamp};

#[derive(Debug)]
pub(crate) struct MySqlTaskRow {
    pub job: Option<Vec<u8>>,
    pub id: Option<String>,
    pub job_type: Option<String>,
    pub status: Option<String>,
    pub attempts: Option<i32>,
    pub max_attempts: Option<i32>,
    pub run_at: Option<DateTime<Utc>>,
    #[allow(unused)]
    pub last_result: Option<String>,
    pub lock_at: Option<DateTime<Utc>>,
    pub lock_by: Option<String>,
    pub done_at: Option<DateTime<Utc>>,
    pub priority: Option<i32>,
    pub idempotency_key: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

impl MySqlTaskRow {
    /// Convert the row into a task, optionally overriding its status.
    pub(crate) fn into_task(self, status: Option<Status>) -> Result<MySqlTask<Vec<u8>>, Error> {
        let mut task = TaskBuilder::new(
            self.job
                .ok_or_else(|| sqlx::Error::ColumnNotFound("job".into()))?,
        )
        .task_id({
            let task_id = self
                .id
                .ok_or_else(|| sqlx::Error::ColumnNotFound("task_id".into()))?;
            TaskId::from_ulid(
                Ulid::from_string(&task_id)
                    .map_err(|e| Error::TaskIdError(Decode(e.to_string())))?,
            )
        })
        .queue(
            self.job_type
                .ok_or_else(|| sqlx::Error::ColumnNotFound("job_type".into()))?
                .into(),
        )
        .status(match status {
            Some(status) => status,
            None => Status::from_str(
                &self
                    .status
                    .ok_or_else(|| sqlx::Error::ColumnNotFound("status".into()))?,
            )
            .map_err(Error::StatusError)?,
        })
        .attempt(
            self.attempts
                .ok_or_else(|| sqlx::Error::ColumnNotFound("attempts".into()))?
                as usize,
        )
        .max_attempts(self.max_attempts.map(|v| v as usize).unwrap_or(25))
        .run_at_timestamp(
            Timestamp::from(
                self.run_at
                    .ok_or(sqlx::Error::ColumnNotFound("run_at".to_owned()))?,
            )
            .0,
        )
        .lock_at(self.lock_at.map(Timestamp::from).map(|ts| ts.0))
        .done_at(self.done_at.map(Timestamp::from).map(|ts| ts.0))
        .lock_by(self.lock_by)
        .priority(self.priority.map(|v| v as usize).unwrap_or_default())
        .with_metadata(
            self.metadata
                .map(|meta| {
                    meta.as_object()
                        .map(|meta| {
                            meta.iter()
                                .map(|(k, v)| {
                                    (k.clone(), v.as_str().map(str::to_owned).unwrap_or_default())
                                })
                                .collect::<HashMap<String, String>>()
                        })
                        .unwrap_or_default()
                })
                .map(MetadataStore::from_map)
                .unwrap_or_default(),
        );

        if let Some(idempotency_key) = self.idempotency_key {
            task = task.idempotency_key(idempotency_key);
        }

        Ok(task.build())
    }
}

impl TryInto<MySqlTask<Vec<u8>>> for MySqlTaskRow {
    type Error = Error;

    fn try_into(self) -> Result<MySqlTask<Vec<u8>>, Self::Error> {
        self.into_task(None)
    }
}
