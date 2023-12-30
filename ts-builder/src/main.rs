use ts_builder::*;

const GUIDE_STR: &str = r#"
ts_builder need an argument which is supposed to be deserialized to TimestampBuilder: 
    {}
or 
    {"start": "2019-09-13 04:00:00Z", "end": "2023-12-30 23:59:59Z", "interval": "15m", "batch_size": 1}
"#;

fn main() -> std::io::Result<()> {
    let mut line = String::new();

    std::io::stdin().read_line(&mut line)?;

    if line.trim().is_empty() {
        println!("{}", GUIDE_STR);
        return Ok(());
    };

    let ts_builder: TimestampBuilder = serde_json::from_str(&line)?;
    let start_times: Vec<_> = DateTimeRange(ts_builder.start, ts_builder.end, ts_builder.interval)
        .step_by(ts_builder.batch_size)
        .collect();
    let datetimes = serde_json::to_string(&start_times)?;
    println!(
        "{{ \"datetimes\": {}, \"interval\": \"{}\", \"batch_size\": {} }}",
        datetimes,
        duration_format::serialize_duration(&ts_builder.interval)?,
        ts_builder.batch_size
    );
    Ok(())
}
