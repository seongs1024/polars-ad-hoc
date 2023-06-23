use crate::chrono_range::TimestampBuilder;
use binance::{api::*, futures::market::*, futures::rest_model::*};
use chrono::Duration;
use futures::stream::{StreamExt, TryStreamExt};
use polars::prelude::*;
use std::{error::Error, io::Cursor, sync::Arc};
use tokio::runtime::Runtime;

pub trait FetchCandleSticks<S1, S2, S3, S4>
where
    S1: AsRef<str>,
    S2: AsRef<str>,
    S3: AsRef<str>,
    S4: AsRef<str>,
{
    type Output;

    pub fn fetch_candlesticks(
        self,
        symbol: S1,
        start: Option<S2>,
        end: Option<S3>,
        interval: S4,
    ) -> Result<Self::Output, Box<dyn Error>>;
}

impl<S1, S2, S3, S4> FetchCandleSticks<S1, S2, S3, S4> for DataFrame
where
    S1: AsRef<str>,
    S2: AsRef<str>,
    S3: AsRef<str>,
    S4: AsRef<str>,
{
    type Output = DataFrame;

    pub fn fetch_candlesticks(
        mut self,
        symbol: S1,
        start: Option<S2>,
        end: Option<S3>,
        interval: S4,
    ) -> Result<Self::Output, Box<dyn Error>> {
        if self.is_empty() {
            let ts_builder =
                TimestampBuilder::new(start.ok_or("no start time")?, end, interval.as_ref())?;
            let df = dataframe(
                ts_builder.build(),
                symbol.as_ref(),
                interval.as_ref(),
                ts_builder.limit as u16,
            );
            return df;
        };
        let start: String = (self["openTime"]
            .datetime()?
            .as_datetime_iter()
            .last()
            .unwrap()
            .unwrap()
            + Duration::minutes(15))
        .format("%Y-%m-%d %H:%M")
        .to_string();
        let ts_builder = TimestampBuilder::new(start, end, interval.as_ref())?;
        let df = dataframe(
            ts_builder.build(),
            symbol.as_ref(),
            interval.as_ref(),
            ts_builder.limit as u16,
        )?;
        if df.is_empty() {
            return Ok(self);
        }
        self.vstack_mut(&df)?;
        self.as_single_chunk_par();
        Ok(self)
    }
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
    S1: AsRef<str>,
    S2: AsRef<str>,
{
    let klines = client
        .get_klines(
            symbol.as_ref(),
            interval.as_ref(),
            limit,
            Some(start),
            Some(end),
        )
        .await?;
    let KlineSummaries::AllKlineSummaries(klines) = klines;
    Ok(klines)
}

fn dataframe<S1, S2>(
    ts: Vec<i64>,
    symbol: S1,
    interval: S2,
    limit: u16,
) -> Result<DataFrame, Box<dyn Error>>
where
    S1: AsRef<str>,
    S2: AsRef<str>,
{
    let market: Arc<FuturesMarket> = Arc::new(Binance::new(None, None));
    let stream = futures::stream::iter(ts.windows(2).into_iter())
        .map(|ts| {
            let market = market.clone();
            let symbol = symbol.as_ref();
            let interval = interval.as_ref();
            request(market, symbol, ts[0] as u64, ts[1] as u64, interval, limit)
        })
        .buffer_unordered(50)
        .map_ok(|rq| futures::stream::iter(rq.into_iter().map(Ok)))
        .try_flatten();
    let rt = Runtime::new()?;
    let candlesticks: Result<Vec<KlineSummary>, Box<dyn Error>> =
        rt.block_on(async move { stream.try_collect().await });

    let json = serde_json::to_string(&candlesticks?).unwrap();
    let cursor = Cursor::new(json);
    let df = JsonReader::new(cursor)
        .finish()?
        .lazy()
        .select([
            col("openTime").cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
            all().exclude(["openTime"]),
        ])
        .sort("openTime", Default::default())
        .collect()?;
    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn df_fetch_from_empty() -> Result<(), Box<dyn Error>> {
        let df = DataFrame::empty()
            .fetch_candlesticks(
                "btcusdt",
                Some("2022-02-01 00:00"),
                Some("2022-02-01 15:00"),
                "15m",
            )
            .unwrap();
        assert_eq!(df.height(), 61);
        Ok(())
    }
    #[test]
    fn df_update_new_fetchs() -> Result<(), Box<dyn Error>> {
        let df = DataFrame::empty()
            .fetch_candlesticks(
                "btcusdt",
                Some("2022-02-01 00:00"),
                Some("2022-02-01 15:00"),
                "15m",
            )
            .unwrap();
        assert_eq!(df.height(), 61);
        let df = df
            .fetch_candlesticks("btcusdt", None::<&str>, Some("2022-02-01 23:59"), "15m")
            .unwrap();
        assert_eq!(df.height(), 96);
        Ok(())
    }

    #[test]
    fn df_ts_short() -> Result<(), Box<dyn std::error::Error>> {
        let ts = TimestampBuilder::new("2023-02-21 00:00", Some("2023-02-21 23:59"), "15m")?;
        let df = dataframe(ts.build(), "btcusdt", "15m", ts.limit as u16)?;
        println!("{}", df);
        assert_eq!(df.height(), 96);
        Ok(())
    }
    #[test]
    fn df_ts_bound() -> Result<(), Box<dyn std::error::Error>> {
        let ts = TimestampBuilder::new("2023-02-21 00:00", Some("2023-02-22 00:00"), "15m")?;
        let df = dataframe(ts.build(), "btcusdt", "15m", ts.limit as u16)?;
        println!("{}", df);
        assert_eq!(df.height(), 97);
        Ok(())
    }
    #[ignore]
    #[test]
    fn df_ts_long() -> Result<(), Box<dyn std::error::Error>> {
        let ts = TimestampBuilder::new("2020-01-01 00:00", Some("2023-02-22 23:59"), "15m")?;
        let df = dataframe(ts.build(), "btcusdt", "15m", ts.limit as u16)?;
        println!("{}", df);
        assert_eq!(df.height(), 110304);
        Ok(())
    }
}
