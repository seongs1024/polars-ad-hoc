use chrono::{Duration, NaiveDateTime, ParseResult, Utc};

use binance::{api::*, futures::market::*, futures::rest_model::*};
use polars::prelude::*;
use std::{io::Cursor, sync::Arc};
use tokio::task::JoinSet;

pub struct TimestampBuilder {
    ts_fmt: String,
    start: i64,
    end: i64,
    step: i64,
    interval: String,
    limit: i64,
}

impl Default for TimestampBuilder {
    fn default() -> Self {
        Self {
            ts_fmt: "%Y-%m-%d %H:%M".into(),
            start: 0,
            end: 0,
            step: 900000,
            interval: "15m".into(),
            limit: 499,
        }
    }
}

impl TimestampBuilder {
    pub fn new<S1, S2, S3>(start: S1, end: Option<S2>, step: S3) -> ParseResult<Self>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
    {
        let mut builder = TimestampBuilder::default();
        builder.start = NaiveDateTime::parse_from_str(start.as_ref(), builder.ts_fmt.as_ref())?
            .timestamp_millis();
        builder.end = match end {
            Some(end) => NaiveDateTime::parse_from_str(end.as_ref(), builder.ts_fmt.as_ref())?
                .timestamp_millis(),
            None => Utc::now().naive_utc().timestamp_millis(),
        };
        builder.interval = step.as_ref().into();
        builder.step = match step.as_ref() {
            "15m" => Duration::minutes(15).num_milliseconds(),
            _ => todo!(),
        };
        Ok(builder)
    }

    pub fn build(&self) -> Option<Vec<i64>> {
        if self.start > self.end {
            return None;
        }
        let mut list: Vec<i64> = (self.start..self.end)
            .step_by((self.step * self.limit) as usize)
            // .map(|ts| NaiveDateTime::from_timestamp_millis(ts).unwrap())
            .collect();
        list.push(self.end);
        Some(list)
    }

    async fn request<S1, S2>(
        client: Arc<FuturesMarket>,
        symbol: S1,
        start: u64,
        end: u64,
        interval: S2,
        limit: u16,
    ) -> Result<Vec<KlineSummary>, Box<dyn std::error::Error>>
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        let klines = client
            .get_klines(symbol, interval, limit, Some(start), Some(end))
            .await?;
        let KlineSummaries::AllKlineSummaries(klines) = klines;
        Ok(klines)
    }

    pub async fn dataframe<S: Into<String> + Copy>(&self, symbol: S) -> PolarsResult<DataFrame> {
        let ts = match self.build() {
            Some(ts) if ts.len() > 1 => ts,
            _ => return Err(PolarsError::NoData("Empty timestamp list".into())),
        };
        let market: Arc<FuturesMarket> = Arc::new(Binance::new(None, None));
        let mut task_set = JoinSet::new();
        ts.windows(2).for_each(|ts| {
            let market = market.clone();
            let symbol = symbol.into();
            let start = ts[0] as u64;
            let end = ts[1] as u64;
            let interval = self.interval.clone();
            let limit = self.limit as u16;
            task_set
                .spawn(async move { Self::request(market, symbol, start, end, interval, limit) });
        });
        let mut candlesticks = Vec::with_capacity(task_set.len());
        while let Some(task) = task_set.join_next().await {
            let klines = task.unwrap().await.unwrap();
            candlesticks.push(klines);
        }
        let candlesticks: Vec<KlineSummary> = candlesticks.into_iter().flatten().collect();
        let json = serde_json::to_string(&candlesticks).unwrap();
        let cursor = Cursor::new(json);
        let df = JsonReader::new(cursor)
            .finish()?
            .lazy()
            .select([
                col("openTime").cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
                all().exclude(["openTime"]),
            ])
            .collect()?;
        Ok(df)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_same_start_end() -> ParseResult<()> {
        let ts = TimestampBuilder::new("2023-02-22 00:00", Some("2023-02-22 00:00"), "15m")?
            .build()
            .unwrap();
        assert_eq!(ts.len(), 1);
        let ts: Vec<_> = ts.windows(2).collect();
        assert_eq!(ts.len(), 0);
        Ok(())
    }
    #[test]
    fn ts_under_limit() -> ParseResult<()> {
        let ts = TimestampBuilder::new("2023-02-22 00:00", Some("2023-02-27 04:44"), "15m")?
            .build()
            .unwrap();
        assert_eq!(ts.len(), 2);
        let ts: Vec<_> = ts.windows(2).collect();
        assert_eq!(ts.len(), 1);
        Ok(())
    }
    #[test]
    fn ts_as_same_as_limit() -> ParseResult<()> {
        let ts = TimestampBuilder::new("2023-02-22 00:00", Some("2023-02-27 04:45"), "15m")?
            .build()
            .unwrap();
        assert_eq!(ts.len(), 2);
        let ts: Vec<_> = ts.windows(2).collect();
        assert_eq!(ts.len(), 1);
        Ok(())
    }
    #[test]
    fn ts_exceed_limit() -> ParseResult<()> {
        let ts = TimestampBuilder::new("2023-02-22 00:00", Some("2023-02-27 04:46"), "15m")?
            .build()
            .unwrap();
        assert_eq!(ts.len(), 3);
        let ts: Vec<_> = ts.windows(2).collect();
        assert_eq!(ts.len(), 2);
        Ok(())
    }
    #[test]
    #[should_panic]
    #[allow(unused)]
    fn ts_panic_parse_str() {
        TimestampBuilder::new("2023-02-22 00:00:00", None::<String>, "15m").unwrap();
    }
    #[test]
    #[should_panic]
    #[allow(unused)]
    fn ts_panic_parse_step() {
        TimestampBuilder::new("2023-02-22 00:00", None::<String>, "1d");
    }
    #[test]
    fn ts_too_long_span() -> ParseResult<()> {
        let ts = TimestampBuilder::new("2020-01-01 00:00", Some("2023-03-01 00:00"), "15m")?
            .build()
            .unwrap();
        assert_eq!(ts.len(), 224);
        let ts: Vec<_> = ts.windows(2).collect();
        assert_eq!(ts.len(), 223);
        Ok(())
    }
    #[test]
    fn ts_start_later_than_end() -> ParseResult<()> {
        let ts =
            TimestampBuilder::new("2023-03-01 01:00", Some("2023-03-01 00:00"), "15m")?.build();
        assert_eq!(ts, None);
        Ok(())
    }

    #[tokio::test]
    async fn ts_df_short() -> Result<(), Box<dyn std::error::Error>> {
        let df = TimestampBuilder::new("2023-02-21 00:00", Some("2023-02-21 23:59"), "15m")
            .unwrap()
            .dataframe("btcusdt")
            .await?;
        println!("{}", df);
        assert_eq!(df.height(), 96);
        Ok(())
    }
    #[tokio::test]
    async fn ts_df_timestamp_bound() -> Result<(), Box<dyn std::error::Error>> {
        let df = TimestampBuilder::new("2023-02-21 00:00", Some("2023-02-22 00:00"), "15m")
            .unwrap()
            .dataframe("btcusdt")
            .await?;
        println!("{}", df);
        assert_eq!(df.height(), 97);
        Ok(())
    }
    #[ignore]
    #[tokio::test]
    async fn ts_df_long() -> Result<(), Box<dyn std::error::Error>> {
        let df = TimestampBuilder::new("2020-01-01 00:00", Some("2023-02-22 23:59"), "15m")
            .unwrap()
            .dataframe("btcusdt")
            .await?;
        println!("{}", df);
        assert_eq!(df.height(), 110304);
        Ok(())
    }
}
