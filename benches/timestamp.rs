use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polars_ad_hoc::chrono_range::TimestampBuilder;

fn gen() {
    let ts = TimestampBuilder::new("2020-01-01 00:00", Some("2023-03-01 00:00"), "15m")
        .unwrap()
        .build();
    let _ts: Vec<_> = ts.windows(2).collect();
}

fn ts_too_long_span(c: &mut Criterion) {
    let mut group = c.benchmark_group("timestamp");
    group.bench_function("ts too long span", |b| b.iter(|| black_box(gen())));
    group.finish();
}

criterion_group!(benches, ts_too_long_span);
criterion_main!(benches);
