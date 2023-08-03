use ts_builder::*;

use chrono::{DateTime, Duration, Utc, TimeZone};
use serde::{Deserialize, Serialize, Serializer, Deserializer};

fn main() -> std::io::Result<()> {
	let mut line = String::new();

	std::io::stdin().read_line(&mut line)?;

	let ts_builder: TimestampBuilder = serde_json::from_str(&line).unwrap();
	let start_times: Vec<_> = DateTimeRange(ts_builder.start, ts_builder.end, ts_builder.interval).step_by(ts_builder.limit).collect();
    let datetimes = serde_json::to_string(&start_times)?;
	println!("{:?}", datetimes);
	Ok(())
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TimestampBuilder {
    // ts_fmt
    #[serde(with = "datetime_format", default = "default_start")]
    start: DateTime<Utc>,
    #[serde(with = "datetime_format", default = "default_end")]
    end: DateTime<Utc>,
    #[serde(with = "duration_format", default = "default_interval")]
    interval: Duration,
	#[serde(default = "default_limit")]
    limit: usize,
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

fn default_limit() -> usize {
    1
}

mod datetime_format {
    use super::*;

    pub fn serialize<S>(
        datetime: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", datetime.format(FORMAT));
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, FORMAT).map_err(serde::de::Error::custom)
    }
}

mod duration_format {
    use super::*;

    pub fn serialize<S>(
        duration: &Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
		let s = match duration.num_minutes() {
			1 => "1m",
			5 => "5m",
			15 => "15m",
			60 => "1h",
			240 => "4h",
			1440 => "1d",
			_ => unimplemented!("not implemented interval"),
		};
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Duration, D::Error>
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
}