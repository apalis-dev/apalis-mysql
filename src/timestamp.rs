//! Datetime wrapper types for MySQL storage backend.
//!
//! This module provides the [`MysqlDateTime`] newtype wrapper that implements
//! the [`SqlTimestamp`] trait from `apalis-sql`. This wrapper is necessary to
//! satisfy Rust's orphan rule while supporting both `chrono` and `time` crates.
//!
//! ## Feature Flags
//!
//! - `chrono` (default): Uses `chrono::DateTime<Utc>` for datetime handling
//! - `time`: Uses `time::OffsetDateTime` for datetime handling
//!
//! When both features are enabled, `time` takes precedence (matching sqlx behavior).

use apalis_sql::SqlTimestamp;
use sqlx::encode::IsNull;
use sqlx::MySql;
use std::fmt;

// ============================================================================
// Chrono implementation (when chrono is enabled but time is not)
// ============================================================================

/// Raw datetime type used for database row decoding.
/// This is what sqlx decodes MySQL DATETIME columns to.
#[cfg(all(feature = "chrono", not(feature = "time")))]
pub(crate) type RawDateTime = chrono::NaiveDateTime;
#[cfg(feature = "time")]
pub(crate) type RawDateTime = time::PrimitiveDateTime;

#[cfg(all(feature = "chrono", not(feature = "time")))]
pub use chrono_impl::*;

#[cfg(all(feature = "chrono", not(feature = "time")))]
mod chrono_impl {
    use super::*;
    use chrono::{DateTime, NaiveDateTime, Utc};

    /// Newtype wrapper around `chrono::DateTime<Utc>` for use with apalis-sql.
    ///
    /// This type implements [`SqlTimestamp`] and all necessary sqlx traits to
    /// enable seamless integration with MySQL datetime columns.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct MysqlDateTime(pub DateTime<Utc>);

    impl SqlTimestamp for MysqlDateTime {
        fn to_unix_timestamp(&self) -> i64 {
            self.0.timestamp()
        }
    }

    impl From<DateTime<Utc>> for MysqlDateTime {
        fn from(dt: DateTime<Utc>) -> Self {
            Self(dt)
        }
    }

    impl From<NaiveDateTime> for MysqlDateTime {
        fn from(dt: NaiveDateTime) -> Self {
            Self(dt.and_utc())
        }
    }

    impl std::ops::Deref for MysqlDateTime {
        type Target = DateTime<Utc>;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl fmt::Display for MysqlDateTime {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt(f)
        }
    }

    // sqlx trait implementations - delegate to inner type
    impl sqlx::Type<MySql> for MysqlDateTime {
        fn type_info() -> <MySql as sqlx::Database>::TypeInfo {
            <NaiveDateTime as sqlx::Type<MySql>>::type_info()
        }

        fn compatible(ty: &<MySql as sqlx::Database>::TypeInfo) -> bool {
            <NaiveDateTime as sqlx::Type<MySql>>::compatible(ty)
        }
    }

    impl<'r> sqlx::Decode<'r, MySql> for MysqlDateTime {
        fn decode(
            value: <MySql as sqlx::Database>::ValueRef<'r>,
        ) -> Result<Self, sqlx::error::BoxDynError> {
            let dt = <NaiveDateTime as sqlx::Decode<'r, MySql>>::decode(value)?;
            Ok(Self(dt.and_utc()))
        }
    }

    impl sqlx::Encode<'_, MySql> for MysqlDateTime {
        fn encode_by_ref(
            &self,
            buf: &mut <MySql as sqlx::Database>::ArgumentBuffer<'_>,
        ) -> Result<IsNull, sqlx::error::BoxDynError> {
            self.0.naive_utc().encode_by_ref(buf)
        }
    }

    /// Get the current datetime in UTC as a [`MysqlDateTime`].
    #[must_use]
    pub(crate) fn now() -> MysqlDateTime {
        MysqlDateTime(Utc::now())
    }

    /// Convert a unix timestamp to a [`MysqlDateTime`].
    #[must_use]
    pub(crate) fn from_unix_timestamp(secs: i64) -> Option<MysqlDateTime> {
        DateTime::from_timestamp(secs, 0).map(MysqlDateTime)
    }
}

// ============================================================================
// Time implementation (when time feature is enabled, takes precedence)
// ============================================================================

#[cfg(feature = "time")]
pub use time_impl::*;

#[cfg(feature = "time")]
mod time_impl {
    use super::*;
    use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

    /// Newtype wrapper around `time::OffsetDateTime` for use with apalis-sql.
    ///
    /// This type implements [`SqlTimestamp`] and all necessary sqlx traits to
    /// enable seamless integration with MySQL datetime columns.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct MysqlDateTime(pub OffsetDateTime);

    impl SqlTimestamp for MysqlDateTime {
        fn to_unix_timestamp(&self) -> i64 {
            self.0.unix_timestamp()
        }
    }

    impl From<OffsetDateTime> for MysqlDateTime {
        fn from(dt: OffsetDateTime) -> Self {
            Self(dt)
        }
    }

    impl From<PrimitiveDateTime> for MysqlDateTime {
        fn from(dt: PrimitiveDateTime) -> Self {
            Self(dt.assume_utc())
        }
    }

    impl std::ops::Deref for MysqlDateTime {
        type Target = OffsetDateTime;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl fmt::Display for MysqlDateTime {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt(f)
        }
    }

    // sqlx trait implementations - delegate to inner type
    impl sqlx::Type<MySql> for MysqlDateTime {
        fn type_info() -> <MySql as sqlx::Database>::TypeInfo {
            <PrimitiveDateTime as sqlx::Type<MySql>>::type_info()
        }

        fn compatible(ty: &<MySql as sqlx::Database>::TypeInfo) -> bool {
            <PrimitiveDateTime as sqlx::Type<MySql>>::compatible(ty)
        }
    }

    impl<'r> sqlx::Decode<'r, MySql> for MysqlDateTime {
        fn decode(
            value: <MySql as sqlx::Database>::ValueRef<'r>,
        ) -> Result<Self, sqlx::error::BoxDynError> {
            let dt = <PrimitiveDateTime as sqlx::Decode<'r, MySql>>::decode(value)?;
            Ok(Self(dt.assume_utc()))
        }
    }

    impl sqlx::Encode<'_, MySql> for MysqlDateTime {
        fn encode_by_ref(
            &self,
            buf: &mut <MySql as sqlx::Database>::ArgumentBuffer<'_>,
        ) -> Result<IsNull, sqlx::error::BoxDynError> {
            // Convert to PrimitiveDateTime for MySQL storage (no timezone)
            let primitive = PrimitiveDateTime::new(
                self.0.to_offset(UtcOffset::UTC).date(),
                self.0.to_offset(UtcOffset::UTC).time(),
            );
            primitive.encode_by_ref(buf)
        }
    }

    /// Get the current datetime in UTC as a [`MysqlDateTime`].
    #[must_use]
    pub(crate) fn now() -> MysqlDateTime {
        MysqlDateTime(OffsetDateTime::now_utc())
    }

    /// Convert a unix timestamp to a [`MysqlDateTime`].
    #[must_use]
    pub(crate) fn from_unix_timestamp(secs: i64) -> Option<MysqlDateTime> {
        OffsetDateTime::from_unix_timestamp(secs).ok().map(MysqlDateTime)
    }
}

// ============================================================================
// Compile-time check: at least one datetime feature must be enabled
// ============================================================================

#[cfg(not(any(feature = "chrono", feature = "time")))]
compile_error!("Either 'chrono' or 'time' feature must be enabled for apalis-mysql");
