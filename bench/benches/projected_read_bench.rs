//! Deterministic evidence for projected Arrow reads versus full-file reads.
//!
//! The benchmark intentionally uses several scalar columns so the projected
//! path has real column bodies to skip. It measures decode time; the fixture's
//! file size and result schemas are recorded alongside runs by the benchmark
//! harness rather than treated as a universal I/O or RSS guarantee.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use strata_storage::{read_batch, read_batch_columns, write_batch};

const ROW_COUNT: usize = 100_000;

struct Fixture {
    path: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "strata-projected-read-bench-{}-{}.arrow",
            std::process::id(),
            std::thread::current().name().unwrap_or("main")
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
            Field::new("extra", DataType::Int64, false),
        ]));
        let ids: Vec<i64> = (0..ROW_COUNT)
            .map(|value| value.try_into().expect("fixture row count fits in i64"))
            .collect();
        let payloads: Vec<String> = (0..ROW_COUNT)
            .map(|value| format!("payload-{value:08}"))
            .collect();
        let extras: Vec<i64> = (0..ROW_COUNT)
            .map(|value| {
                let value: i64 = value.try_into().expect("fixture row count fits in i64");
                value * 3
            })
            .collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payloads)),
                Arc::new(Int64Array::from(extras)),
            ],
        )
        .expect("fixture arrays have matching lengths");
        write_batch(&path, &batch).expect("fixture write succeeds");
        Self { path }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn bench_projected_reads(c: &mut Criterion) {
    let fixture = Fixture::new();
    let file_bytes = std::fs::metadata(&fixture.path)
        .expect("fixture metadata exists")
        .len();
    eprintln!(
        "projected-read fixture: rows={ROW_COUNT} file_bytes={file_bytes} projected_columns=1"
    );

    let mut group = c.benchmark_group("projected_reads");
    group.bench_function("full_file", |b| {
        b.iter(|| {
            let batch = read_batch(&fixture.path).expect("full fixture read succeeds");
            assert_eq!(batch.num_columns(), 3);
            std::hint::black_box(batch);
        });
    });
    group.bench_function("one_column", |b| {
        b.iter(|| {
            let batch = read_batch_columns(&fixture.path, &["id"])
                .expect("projected fixture read succeeds");
            assert_eq!(batch.num_columns(), 1);
            std::hint::black_box(batch);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_projected_reads);
criterion_main!(benches);
