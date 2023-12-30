use chrono::{DateTime, Duration, TimeZone, Utc};
use cs_fetcher::*;
use polars::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::error::Error;
use ts_builder::*;

const GUIDE_STR1: &str = r#"
cs-fetcher needs ts-builder's output!
"#;
const GUIDE_STR2: &str = r#"
cs-fetcher needs an SYMBOL argument!
"#;

fn main() -> Result<(), Box<dyn Error>> {
    let mut line = String::new();

    std::io::stdin().read_line(&mut line)?;
    if line.trim().is_empty() {
        println!("{}", GUIDE_STR1);
        return Ok(());
    };

    let timestamps: Timestamps = serde_json::from_str(&line).unwrap();

    // std::io::stdin().read_line(&mut line)?;
    // if line.trim().is_empty() {
    //     println!("{}", GUIDE_STR2);
    //     return Ok(());
    // };

    let symbol = "btcusdt"; // line.trim();

    let ts: Vec<i64> = timestamps
        .datetimes
        .iter()
        .map(|dt| dt.timestamp_millis())
        .collect();
    let mut df = dataframe(
        ts,
        &symbol,
        &timestamps.interval,
        timestamps.batch_size as u16,
    )?;

    let fs = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(format!(
            "{symbol}-perp-binance-{}-from-{}-to-{}.parquet",
            timestamps.interval,
            format!(
                "{}",
                timestamps
                    .datetimes
                    .first()
                    .unwrap()
                    .format("%Y-%m-%d-%H-%M")
            ),
            format!(
                "{}",
                timestamps
                    .datetimes
                    .last()
                    .unwrap()
                    .format("%Y-%m-%d-%H-%M")
            )
        ))?;
    ParquetWriter::new(fs).finish(&mut df).unwrap();

    Ok(())
}

#[derive(Deserialize, Serialize, Debug)]
struct Timestamps {
    pub datetimes: Vec<DateTime<Utc>>,
    pub interval: String,
    pub batch_size: usize,
}
