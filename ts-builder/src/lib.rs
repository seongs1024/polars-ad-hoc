use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub struct DateTimeRange(pub DateTime<Utc>, pub DateTime<Utc>, pub Duration);

impl Iterator for DateTimeRange {
    type Item = DateTime<Utc>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.0 <= self.1 {
            let next = self.0 + self.2;
            Some(std::mem::replace(&mut self.0, next))
        } else {
            None
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TimestampBuilder {
    // ts_fmt
    // #[serde(with = "datetime_format", default = "default_start")]
    #[serde(default = "default_start")]
    pub start: DateTime<Utc>,
    // #[serde(with = "datetime_format", default = "default_end")]
    #[serde(default = "default_end")]
    pub end: DateTime<Utc>,
    #[serde(with = "duration_format", default = "default_interval")]
    pub interval: Duration,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

const FORMAT: &'static str = "%Y-%m-%d %H:%M";

pub fn default_start() -> DateTime<Utc> {
    Utc.datetime_from_str("2019-09-13 04:00", FORMAT).unwrap()
}

pub fn default_end() -> DateTime<Utc> {
    Utc::now()
}

pub fn default_interval() -> Duration {
    Duration::milliseconds(900000)
}

pub fn default_batch_size() -> usize {
    499
}

pub mod duration_format {
    use super::*;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = serialize_duration(duration).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "1m" => Ok(Duration::milliseconds(60000)),
            "5m" => Ok(Duration::milliseconds(300000)),
            "15m" => Ok(Duration::milliseconds(900000)),
            "1h" => Ok(Duration::milliseconds(3600000)),
            "4h" => Ok(Duration::milliseconds(14400000)),
            "1d" => Ok(Duration::milliseconds(86400000)),
            _ => Err(serde::de::Error::custom("unexpected time interval")),
        }
    }

    pub fn serialize_duration(duration: &Duration) -> std::io::Result<&'static str> {
        match duration.num_minutes() {
            1 => Ok("1m"),
            5 => Ok("5m"),
            15 => Ok("15m"),
            60 => Ok("1h"),
            240 => Ok("4h"),
            1440 => Ok("1d"),
            _ => unimplemented!("not implemented interaval"),
        }
    }
}
