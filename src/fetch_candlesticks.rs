use crate::chrono_range::TimestampBuilder;
use chrono::Duration;
use polars::prelude::*;
use std::error::Error;
use tokio::runtime::Runtime;

pub trait FetchCandleSticks<S1, S2, S3, S4>
where
    S1: AsRef<str> + Copy,
    S2: AsRef<str> + Copy,
    S3: AsRef<str> + Copy,
    S4: AsRef<str> + Copy,
{
    type Output;

    fn fetch_candlesticks(
        self,
        symbol: S1,
        start: Option<S2>,
        end: Option<S3>,
        interval: S4,
    ) -> Result<Self::Output, Box<dyn Error>>;
}

impl<S1, S2, S3, S4> FetchCandleSticks<S1, S2, S3, S4> for DataFrame
where
    S1: AsRef<str> + Copy,
    S2: AsRef<str> + Copy,
    S3: AsRef<str> + Copy,
    S4: AsRef<str> + Copy,
{
    type Output = DataFrame;

    fn fetch_candlesticks(
        mut self,
        symbol: S1,
        start: Option<S2>,
        end: Option<S3>,
        interval: S4,
    ) -> Result<Self::Output, Box<dyn Error>> {
        if self.is_empty() {
            let ts_builder = TimestampBuilder::new(start.ok_or("no start time")?, end, interval)?;
            let rt = Runtime::new().unwrap();
            let df = rt.block_on(async { ts_builder.dataframe(symbol.as_ref()).await });
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
        let ts_builder = TimestampBuilder::new(start, end, interval)?;
        let rt = Runtime::new().unwrap();
        let df = rt.block_on(async { ts_builder.dataframe(symbol.as_ref()).await })?;
        if df.is_empty() {
            return Ok(self);
        }
        self.vstack_mut(&df)?;
        self.as_single_chunk_par();
        Ok(self)
    }
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
}
