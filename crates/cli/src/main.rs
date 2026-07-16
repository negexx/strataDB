//! `strata` CLI — dataset/manifest inspection, and the Phase 1 MVP checklist
//! surface. `crash-loop` exists specifically to be killed mid-write by
//! `crates/txn/tests/mvp_checklist.rs`'s crash-recovery test (checklist step
//! 6): it commits one row at a time, printing (and flushing) "committed N"
//! after each success, so an external harness can kill it deterministically
//! partway through and verify recovery.

use std::env;
use std::error::Error;
use std::io::Write as _;
use std::process::ExitCode;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;

fn mvp_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        ),
    ]))
}

fn make_row(id: i64, name: &str, vector: [f32; 3]) -> Result<RecordBatch, ArrowError> {
    let id_arr: Arc<dyn Array> = Arc::new(Int64Array::from(vec![id]));
    let name_arr: Arc<dyn Array> = Arc::new(StringArray::from(vec![name.to_string()]));
    let item_field = Arc::new(Field::new("item", DataType::Float32, false));
    let values = Arc::new(Float32Array::from(vector.to_vec()));
    let vec_arr: Arc<dyn Array> = Arc::new(FixedSizeListArray::new(item_field, 3, values, None));
    RecordBatch::try_new(mvp_schema(), vec![id_arr, name_arr, vec_arr])
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<(), Box<dyn Error>> {
    let Some(cmd) = args.get(1) else {
        eprintln!(
            "usage: strata <create|insert|scan|filter|search|inspect|crash-loop> <dir> [...]"
        );
        return Ok(());
    };
    let dir = args.get(2).ok_or("missing <dir> argument")?;

    match cmd.as_str() {
        "create" => {
            strata_txn::Dataset::create(dir)?;
            println!("created dataset at {dir}");
        }
        "insert" => {
            let id: i64 = args.get(3).ok_or("missing <id>")?.parse()?;
            let name = args.get(4).ok_or("missing <name>")?;
            let v0: f32 = args.get(5).ok_or("missing <v0>")?.parse()?;
            let v1: f32 = args.get(6).ok_or("missing <v1>")?.parse()?;
            let v2: f32 = args.get(7).ok_or("missing <v2>")?.parse()?;
            let ds = strata_txn::Dataset::open(dir)?;
            let mut txn = ds.begin();
            txn.insert(make_row(id, name, [v0, v1, v2])?);
            let ds = txn.commit()?;
            println!("committed version {}", ds.current_version());
        }
        "scan" => {
            let ds = strata_txn::Dataset::open(dir)?;
            let batch = ds.scan(&mvp_schema())?;
            println!(
                "{} rows at version {}",
                batch.num_rows(),
                ds.current_version()
            );
            print_batch(&batch)?;
        }
        "filter" => {
            let name = args.get(3).ok_or("missing <name>")?;
            let ds = strata_txn::Dataset::open(dir)?;
            let batch = ds.scan(&mvp_schema())?;
            let filtered = strata_query::filter_eq(&batch, "name", name)?;
            println!("{} matching rows", filtered.num_rows());
            print_batch(&filtered)?;
        }
        "search" => {
            let v0: f32 = args.get(3).ok_or("missing <v0>")?.parse()?;
            let v1: f32 = args.get(4).ok_or("missing <v1>")?.parse()?;
            let v2: f32 = args.get(5).ok_or("missing <v2>")?.parse()?;
            let k: usize = args.get(6).map(|s| s.parse()).transpose()?.unwrap_or(3);
            let ds = strata_txn::Dataset::open(dir)?;
            let batch = ds.scan(&mvp_schema())?;
            let vec_idx = batch.schema_ref().index_of("vector")?;
            let vectors = batch
                .column(vec_idx)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or("vector column has wrong type")?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("id column has wrong type")?;
            for n in strata_index::brute_force_search(vectors, &[v0, v1, v2], k)? {
                println!(
                    "id={} squared_distance={}",
                    ids.value(n.row_index),
                    n.squared_distance
                );
            }
        }
        "inspect" => {
            let ds = strata_txn::Dataset::open(dir)?;
            let batch = ds.scan(&mvp_schema())?;
            println!(
                "version={} row_count={}",
                ds.current_version(),
                batch.num_rows()
            );
        }
        "crash-loop" => {
            let n: usize = args.get(3).ok_or("missing <num_commits>")?.parse()?;
            let mut ds = strata_txn::Dataset::open(dir)?;
            for i in 0..n {
                let mut txn = ds.begin();
                #[allow(clippy::cast_precision_loss)]
                txn.insert(make_row(i64::try_from(i)?, "loop", [i as f32, 0.0, 0.0])?);
                ds = txn.commit()?;
                println!("committed {}", ds.current_version());
                std::io::stdout().flush()?;
            }
        }
        other => return Err(format!("unknown command: {other}").into()),
    }

    Ok(())
}

fn print_batch(batch: &RecordBatch) -> Result<(), Box<dyn Error>> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("id column has wrong type")?;
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("name column has wrong type")?;
    for i in 0..batch.num_rows() {
        println!("  id={} name={}", ids.value(i), names.value(i));
    }
    Ok(())
}
