//! PERF-07 evidence for indexed row-group selective reads.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use strata_storage::{read_row_groups, row_group_index, write_row_groups};

const ROW_COUNT: usize = 100_000;
const GROUP_ROWS: usize = 5_000;

struct Fixture {
    path: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "strata-row-group-bench-{}-{}.arrow",
            std::process::id(),
            std::thread::current().name().unwrap_or("main")
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let ids = (0..ROW_COUNT)
            .map(|value| i64::try_from(value).expect("benchmark fixture fits in i64"))
            .collect::<Vec<_>>();
        let payload = (0..ROW_COUNT)
            .map(|value| format!("payload-{value:08}"))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payload)),
            ],
        )
        .expect("fixture arrays have matching lengths");
        write_row_groups(&path, &batch, GROUP_ROWS).expect("row-group write succeeds");
        Self { path }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn bench_row_groups(c: &mut Criterion) {
    let fixture = Fixture::new();
    let index = row_group_index(&fixture.path).expect("row-group index exists");
    let file_bytes = std::fs::metadata(&fixture.path)
        .expect("fixture metadata exists")
        .len();
    let selected_bytes: u64 = index[0..1].iter().map(|entry| entry.byte_len).sum();
    eprintln!(
        "row-group fixture: rows={ROW_COUNT} groups={} group_rows={GROUP_ROWS} file_bytes={file_bytes} selected_payload_bytes={selected_bytes}",
        index.len()
    );

    let mut group = c.benchmark_group("row_group_reads");
    group.bench_function("all_groups_one_column", |b| {
        b.iter(|| {
            let batch = read_row_groups(&fixture.path, &["id"], 0..index.len())
                .expect("full row-group read succeeds");
            assert_eq!(batch.num_rows(), ROW_COUNT);
            std::hint::black_box(batch);
        });
    });
    group.bench_function("one_group_one_column", |b| {
        b.iter(|| {
            let batch = read_row_groups(&fixture.path, &["id"], 0..1)
                .expect("selective row-group read succeeds");
            assert_eq!(batch.num_rows(), GROUP_ROWS);
            std::hint::black_box(batch);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_row_groups);
criterion_main!(benches);
