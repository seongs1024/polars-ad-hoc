use chrono::{Duration, NaiveDateTime, ParseResult, Utc};

pub struct TimestampBuilder {
    ts_fmt: String,
    start: i64,
    end: i64,
    step: i64,
    limit: i64,
}

impl Default for TimestampBuilder {
    fn default() -> Self {
        Self {
            ts_fmt: "%Y-%m-%d %H:%M".to_owned(),
            start: 0,
            end: 0,
            step: 900000,
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
}
