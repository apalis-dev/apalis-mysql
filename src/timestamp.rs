use sqlx::types::chrono::{DateTime, Utc};

/// A point in time, stored as seconds since the Unix epoch.
///
/// The crate keeps timestamps as plain `u64` seconds so that no date and time
/// library leaks into the public API. MySQL stores `TIMESTAMP` columns in
/// UTC, so the conversion is lossless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Timestamp(pub u64);

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Timestamp(value.timestamp().max(0) as u64)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(value: Timestamp) -> Self {
        DateTime::from_timestamp(value.0 as i64, 0).unwrap_or_else(Utc::now)
    }
}

impl Timestamp {
    /// The current time.
    #[must_use]
    pub fn now() -> Self {
        Timestamp(Utc::now().timestamp().max(0) as u64)
    }
}
