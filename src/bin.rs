use fetch_candlesticks_traits::fetch_candlesticks::FetchCandleSticks;
use polars::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let fs = std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open("btcusdt-perp-binance-15m-from-2019-09-13-04-00.parquet")?;
	let mut df = DataFrame::empty()
		.fetch_candlesticks(
			"btcusdt",
			Some("2019-09-13 04:00"),
			None::<&str>,
			"15m",
		)
		.unwrap();	
	ParquetWriter::new(fs).finish(&mut df).unwrap();
	Ok(())
}