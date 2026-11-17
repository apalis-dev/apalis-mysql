use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::{
    Decode, Encode, MySql, Type, TypeInfo, ValueRef,
    encode::IsNull,
    error::BoxDynError,
    mysql::{MySqlTypeInfo, MySqlValueRef},
};

/// A point in time, stored as seconds since the Unix epoch.
///
/// The crate keeps timestamps as plain `u64` seconds so that no date and time
/// library leaks into the public API. MySQL stores `TIMESTAMP` columns in
/// UTC, so the conversion is lossless.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Timestamp(pub u64);

impl Timestamp {
    /// The current time.
    #[must_use]
    pub fn now() -> Self {
        // A clock set before 1970 is treated as the epoch itself.
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Timestamp(secs)
    }

    /// Seconds since the Unix epoch.
    #[must_use]
    pub const fn as_secs(self) -> u64 {
        self.0
    }
}

impl From<u64> for Timestamp {
    fn from(secs: u64) -> Self {
        Timestamp(secs)
    }
}

impl From<Timestamp> for u64 {
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

fn is_datetime_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("TIMESTAMP") || name.eq_ignore_ascii_case("DATETIME")
}

impl Type<MySql> for Timestamp {
    fn type_info() -> MySqlTypeInfo {
        <String as Type<MySql>>::type_info()
    }

    fn compatible(ty: &MySqlTypeInfo) -> bool {
        is_datetime_name(ty.name()) || <u64 as Type<MySql>>::compatible(ty)
    }
}

impl Encode<'_, MySql> for Timestamp {
    fn encode_by_ref(&self, buf: &mut Vec<u8>) -> Result<IsNull, BoxDynError> {
        let secs = i64::try_from(self.0)?;
        let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
        let (y, m, d) = civil_from_days(days);
        let s = format!(
            "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
            rem / 3_600,
            rem % 3_600 / 60,
            rem % 60
        );
        <String as Encode<MySql>>::encode(s, buf)
    }
}

impl<'r> Decode<'r, MySql> for Timestamp {
    fn decode(value: MySqlValueRef<'r>) -> Result<Self, BoxDynError> {
        // Plain integer columns (e.g. `UNIX_TIMESTAMP(col)` cast to UNSIGNED).
        let is_datetime = is_datetime_name(value.type_info().name());
        if !is_datetime {
            return Ok(Timestamp(<u64 as Decode<MySql>>::decode(value)?));
        }

        let bytes = <&[u8] as Decode<MySql>>::decode(value)?;

        // Binary protocol payloads are at most 12 bytes (length prefix + 11);
        // text protocol values ("YYYY-MM-DD HH:MM:SS") are at least 19.
        let (y, mo, d, h, mi, s) = if bytes.len() <= 12 {
            parse_binary(bytes)?
        } else {
            parse_text(std::str::from_utf8(bytes)?)?
        };

        if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
            return Err("zero or invalid date in TIMESTAMP value".into());
        }

        let days = days_from_civil(y, mo, d);
        let secs = days * 86_400 + h * 3_600 + mi * 60 + s;
        Ok(Timestamp(
            u64::try_from(secs).map_err(|_| "timestamp before 1970")?,
        ))
    }
}

type Parts = (i64, i64, i64, i64, i64, i64);

/// Binary protocol: `len, year(u16 LE), month, day, [hour, minute, second, [micros(u32 LE)]]`.
fn parse_binary(b: &[u8]) -> Result<Parts, BoxDynError> {
    let len = *b.first().ok_or("empty TIMESTAMP value")? as usize;
    if len == 0 {
        return Err("zero date in TIMESTAMP value".into());
    }
    if b.len() < 1 + len || len < 4 {
        return Err("malformed binary TIMESTAMP value".into());
    }
    let year = i64::from(u16::from_le_bytes([b[1], b[2]]));
    let (month, day) = (i64::from(b[3]), i64::from(b[4]));
    let (h, m, s) = if len >= 7 {
        (i64::from(b[5]), i64::from(b[6]), i64::from(b[7]))
    } else {
        (0, 0, 0)
    };
    Ok((year, month, day, h, m, s))
}

/// Text protocol: `YYYY-MM-DD HH:MM:SS[.ffffff]`.
fn parse_text(s: &str) -> Result<Parts, BoxDynError> {
    let field = |r: std::ops::Range<usize>| -> Result<i64, BoxDynError> {
        Ok(s.get(r).ok_or("malformed TIMESTAMP text")?.parse()?)
    };
    Ok((
        field(0..4)?,
        field(5..7)?,
        field(8..10)?,
        field(11..13)?,
        field(14..16)?,
        field(17..19)?,
    ))
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12; // March = 0
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}
