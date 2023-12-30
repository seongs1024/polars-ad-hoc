use ts_builder::*;

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const GUIDE_STR: &str = r#"
ts_builder need an argument which is supposed to be deserialized to TimestampBuilder: 
    {}
or 
    {"start": "2019-09-13 04:00", "end": "2023-12-30 23:00", "interval": "15m", "batch_size": 1}
"#;

fn main() -> std::io::Result<()> {
    let mut line = String::new();

    std::io::stdin().read_line(&mut line)?;

    if line.trim().is_empty() {
        println!("{}", GUIDE_STR);
        return Ok(())
    };

    let ts_builder: TimestampBuilder = serde_json::from_str(&line)?;
    let start_times: Vec<_> = DateTimeRange(ts_builder.start, ts_builder.end, ts_builder.interval).step_by(ts_builder.batch_size).collect();
    let datetimes = serde_json::to_string(&start_times)?;
    println!("{{ \"datetimes\": {}, \"interval\": \"{}\", \"batch_size\": {} }}",
        datetimes,
        duration_format::serialize_duration(&ts_builder.interval)?,
        ts_builder.batch_size
    );
    Ok(())
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TimestampBuilder {
    // ts_fmt
    // #[serde(with = "datetime_format", default = "default_start")]
    #[serde(default = "default_start")]
    start: DateTime<Utc>,
    // #[serde(with = "datetime_format", default = "default_end")]
    #[serde(default = "default_end")]
    end: DateTime<Utc>,
    #[serde(with = "duration_format", default = "default_interval")]
    interval: Duration,
    #[serde(default = "default_batch_size")]
    batch_size: usize,
}

const FORMAT: &'static str = "%Y-%m-%d %H:%M";

fn default_start() -> DateTime<Utc> {
    Utc.datetime_from_str("2019-09-13 04:00", FORMAT).unwrap()
}

fn default_end() -> DateTime<Utc> {
    Utc::now()
}

fn default_interval() -> Duration {
    Duration::milliseconds(900000)
}

fn default_batch_size() -> usize {
    1
}

mod duration_format {
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
