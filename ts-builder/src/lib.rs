use chrono::{DateTime, Duration, Utc};

pub struct DateTimeRange(pub DateTime<Utc>, pub DateTime<Utc>, pub Duration);

impl Iterator for DateTimeRange {
    type Item = DateTime<Utc>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.0 <= self.1 {
            let next = self.0 + self.2;
            Some(std::mem::replace(&mut self.0, next))
        } else {
            None
        }
    }
}
