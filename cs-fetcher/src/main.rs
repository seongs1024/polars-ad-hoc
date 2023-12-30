use chrono::{DateTime, Duration, TimeZone, Utc};
use cs_fetcher::*;
use polars::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::error::Error;
use ts_builder::*;

const GUIDE_STR: &str = r#"
cs-fetcher needs arguments!

{"symbol": "btcusdt", "start": "2023-12-30 00:00:00Z", "interval": "4h"}
"#;

fn main() -> Result<(), Box<dyn Error>> {
    let mut line = String::new();

    std::io::stdin().read_line(&mut line)?;
    if line.trim().is_empty() {
        println!("{}", GUIDE_STR);
        return Ok(());
    };

    let Requirements{symbol, start, end, interval, batch_size} = serde_json::from_str::<Requirements>(&line)?;
    let start_times: Vec<_> = DateTimeRange(start, end, interval)
        .step_by(batch_size)
        .chain(std::iter::once(end))
        .collect();
    let interval = duration_format::serialize_duration(&interval)?;

    let ts: Vec<i64> = start_times
        .iter()
        .map(|dt| dt.timestamp_millis())
        .collect();
    let mut df = dataframe(
        ts,
        &symbol,
        &interval,
        batch_size as u16,
    )?;

    let fs = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(format!(
            "{symbol}-perp-binance-{}-from-{}-to-{}.parquet",
            interval,
            format!(
                "{}",
                start_times
                    .first()
                    .unwrap()
                    .format("%Y-%m-%d-%H-%M")
            ),
            format!(
                "{}",
                start_times
                    .last()
                    .unwrap()
                    .format("%Y-%m-%d-%H-%M")
            )
        ))?;
    println!("{}", df);
    ParquetWriter::new(fs).finish(&mut df).unwrap();
    Ok(())
}

#[derive(Deserialize, Serialize, Debug)]
struct Requirements {
    #[serde(default = "default_symbol")]
    pub symbol: String,
    #[serde(default = "default_start")]
    pub start: DateTime<Utc>,
    #[serde(default = "default_end")]
    pub end: DateTime<Utc>,
    #[serde(with = "duration_format", default = "default_interval")]
    pub interval: Duration,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

pub fn default_symbol() -> String {
    "btcusdt".to_owned()
}
