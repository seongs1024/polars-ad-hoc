use binance::{api::*, futures::market::*, futures::rest_model::*};
use futures::stream::{StreamExt, TryStreamExt};
use polars::prelude::*;
use std::{error::Error, io::Cursor, sync::Arc};
use tokio::runtime::Runtime;

pub fn dataframe<S1, S2>(
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

    let candlesticks = candlesticks?;

    let json = serde_json::to_string(&candlesticks).unwrap();
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
