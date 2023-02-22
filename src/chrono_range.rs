use chrono::{Duration, NaiveDateTime, ParseResult, Utc};

#[allow(dead_code)]
pub fn timestamp<S1, S2, S3>(
    start: S1,
    end: Option<S2>,
    step: S3,
    limit: i64,
) -> ParseResult<Vec<i64>>
where
    S1: AsRef<str>,
    S2: AsRef<str>,
    S3: AsRef<str>,
{
    let ts_fmt = "%Y-%m-%d %H:%M";

    let start = NaiveDateTime::parse_from_str(start.as_ref(), ts_fmt)?;
    let end = match end {
        Some(end) => NaiveDateTime::parse_from_str(end.as_ref(), ts_fmt)?,
        None => Utc::now().naive_utc(),
    };
    let step = match step.as_ref() {
        "15m" => Duration::minutes(15),
        _ => todo!(),
    };

    let mut list: Vec<i64> = (start.timestamp_millis()..end.timestamp_millis())
        .step_by((step.num_milliseconds() * limit) as usize)
        // .map(|ts| NaiveDateTime::from_timestamp_millis(ts).unwrap())
        .collect();
    list.push(end.timestamp_millis());
    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_under_limit() {
        let ts = timestamp("2023-02-22 00:00", Some("2023-02-27 04:44"), "15m", 499).unwrap();
        let ts: Vec<_> = ts.windows(2).collect();
        assert_eq!(ts.len(), 1);
    }
    #[test]
    fn ts_as_same_as_limit() {
        let ts = timestamp("2023-02-22 00:00", Some("2023-02-27 04:45"), "15m", 499).unwrap();
        let ts: Vec<_> = ts.windows(2).collect();
        assert_eq!(ts.len(), 1);
    }
    #[test]
    fn ts_exceed_limit() {
        let ts = timestamp("2023-02-22 00:00", Some("2023-02-27 04:46"), "15m", 499).unwrap();
        let ts: Vec<_> = ts.windows(2).collect();
        assert_eq!(ts.len(), 2);
    }
    #[test]
    #[should_panic]
    fn ts_panic_parse_str() {
        timestamp("2023-02-22 00:00:00", None::<String>, "15m", 499).unwrap();
    }
    #[test]
    #[should_panic]
    fn ts_panic_parse_step() {
        timestamp("2023-02-22 00:00", None::<String>, "1d", 499).unwrap();
    }
    #[test]
    fn ts_too_long_span() {
        let ts = timestamp("2020-01-01 00:00", Some("2023-03-01 00:00"), "15m", 499).unwrap();
        assert_eq!(ts.len(), 224);
        let ts: Vec<_> = ts.windows(2).collect();
        assert_eq!(ts.len(), 223);
    }
}
