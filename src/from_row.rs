use chrono::NaiveDateTime;
use serde_json::Value;

#[derive(Debug)]
pub(crate) struct MySqlTaskRow {
    pub(crate) job: Vec<u8>,
    pub(crate) id: Option<String>,
    pub(crate) job_type: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) attempts: Option<i32>,
    pub(crate) max_attempts: Option<i32>,
    pub(crate) run_at: Option<NaiveDateTime>,
    pub(crate) last_result: Option<String>,
    pub(crate) lock_at: Option<NaiveDateTime>,
    pub(crate) lock_by: Option<String>,
    pub(crate) done_at: Option<NaiveDateTime>,
    pub(crate) priority: Option<i32>,
    pub(crate) metadata: Option<Value>,
}

impl TryInto<apalis_sql::from_row::TaskRow> for MySqlTaskRow {
    type Error = sqlx::Error;

    fn try_into(self) -> Result<apalis_sql::from_row::TaskRow, Self::Error> {
        Ok(apalis_sql::from_row::TaskRow {
            job: self.job,
            id: self
                .id
                .ok_or_else(|| sqlx::Error::Protocol("Missing id".into()))?,
            job_type: self
                .job_type
                .ok_or_else(|| sqlx::Error::Protocol("Missing job_type".into()))?,
            status: self
                .status
                .ok_or_else(|| sqlx::Error::Protocol("Missing status".into()))?,
            attempts: self
                .attempts
                .ok_or_else(|| sqlx::Error::Protocol("Missing attempts".into()))?
                as usize,
            max_attempts: self.max_attempts.map(|v| v as usize),
            run_at: self.run_at.map(|ts| ts.and_utc()),
            last_result: self
                .last_result
                .map(|res| serde_json::from_str(&res).unwrap_or(serde_json::Value::Null)),
            lock_at: self.lock_at.map(|ts| ts.and_utc()),
            lock_by: self.lock_by,
            done_at: self.done_at.map(|ts| ts.and_utc()),
            priority: self.priority.map(|v| v as usize),
            metadata: self.metadata,
        })
    }
}
