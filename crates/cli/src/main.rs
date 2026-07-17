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
    const KNOWN_COMMANDS: &[&str] = &[
        "create",
        "insert",
        "scan",
        "filter",
        "search",
        "inspect",
        "explain",
        "crash-loop",
    ];

    let Some(cmd) = args.get(1) else {
        eprintln!(
            "usage: strata <create|insert|scan|filter|search|explain|inspect|crash-loop> <dir> [...]"
        );
        eprintln!("  search <dir> <v0> <v1> <v2> [k] [--exact] [--filter <column> <op> <value>]");
        return Ok(());
    };
    if !KNOWN_COMMANDS.contains(&cmd.as_str()) {
        return Err(format!("unknown command: {cmd}").into());
    }

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
        "search" => handle_search(args, dir)?,
        "inspect" => {
            let ds = strata_txn::Dataset::open(dir)?;
            let batch = ds.scan(&mvp_schema())?;
            println!(
                "version={} row_count={}",
                ds.current_version(),
                batch.num_rows()
            );
        }
        "explain" => {
            handle_explain(dir, args)?;
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

/// Resolves each vector-search match's row-id to its display `(id,
/// squared_distance)` pair, using a `HashMap` built once so lookup is
/// O(n+k) rather than an O(k*n) nested scan over every row for every
/// match. Matches whose row-id isn't found in `row_ids` (which should
/// never happen in practice, since `matches` come from searching the same
/// index `row_ids` was scanned from) are silently skipped, same as the
/// original nested-loop behavior.
fn resolve_display_rows(
    matches: &[strata_index::VectorMatch],
    row_ids: &arrow::array::UInt64Array,
    ids: &Int64Array,
) -> Vec<(i64, f32)> {
    let row_index_by_id: std::collections::HashMap<u64, usize> = (0..row_ids.len())
        .map(|row| (row_ids.value(row), row))
        .collect();
    matches
        .iter()
        .filter_map(|m| {
            row_index_by_id
                .get(&m.row_id)
                .map(|&row| (ids.value(row), m.squared_distance))
        })
        .collect()
}

fn handle_search(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    let exact = args.iter().any(|a| a == "--exact");
    let filter_idx = args.iter().position(|a| a == "--filter");

    let positional: Vec<&String> = args
        .iter()
        .skip(3)
        .take_while(|a| !a.starts_with("--"))
        .collect();
    let v0: f32 = positional.first().ok_or("missing <v0>")?.parse()?;
    let v1: f32 = positional.get(1).ok_or("missing <v1>")?.parse()?;
    let v2: f32 = positional.get(2).ok_or("missing <v2>")?.parse()?;
    let k: usize = positional
        .get(3)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(3);

    let predicate = match filter_idx {
        Some(idx) => {
            let column = args.get(idx + 1).ok_or("missing <column> after --filter")?;
            let op = args.get(idx + 2).ok_or("missing <op> after --filter")?;
            let value: i64 = args
                .get(idx + 3)
                .ok_or("missing <value> after --filter")?
                .parse()?;
            Some(parse_predicate(column, op, value)?)
        }
        None => None,
    };

    let ds = strata_txn::Dataset::open(dir)?;

    if exact {
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
        return Ok(());
    }

    let matches = ds.vector_search(&[v0, v1, v2], k, predicate.as_ref())?;

    // Scan once, requesting the hidden row-id column back, to translate
    // vector_search's row-ids into the user-facing id/name columns for
    // display — matches this project's "Dataset doesn't translate row-ids
    // back to column values, that's the caller's job" design (see
    // .claude/docs/design/phase-4-vector-index-spec.md §3).
    let mut display_fields = mvp_schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    display_fields.push(Field::new(
        strata_txn::ROW_ID_COLUMN,
        DataType::UInt64,
        false,
    ));
    let display_schema = Arc::new(Schema::new(display_fields));
    let batch = ds.scan(&display_schema)?;
    let row_id_idx = batch.schema_ref().index_of(strata_txn::ROW_ID_COLUMN)?;
    let row_ids = batch
        .column(row_id_idx)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .ok_or("row-id column has wrong type")?;
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("id column has wrong type")?;

    for (id, squared_distance) in resolve_display_rows(&matches, row_ids, ids) {
        println!("id={id} squared_distance={squared_distance}");
    }
    Ok(())
}

fn parse_predicate(
    column: &str,
    op: &str,
    value: i64,
) -> Result<strata_query::Predicate, Box<dyn Error>> {
    use strata_query::Predicate;
    use strata_storage::Value;
    match op {
        "eq" => Ok(Predicate::Eq(column.to_string(), Value::Int64(value))),
        "lt" => Ok(Predicate::Lt(column.to_string(), Value::Int64(value))),
        "lteq" => Ok(Predicate::LtEq(column.to_string(), Value::Int64(value))),
        "gt" => Ok(Predicate::Gt(column.to_string(), Value::Int64(value))),
        "gteq" => Ok(Predicate::GtEq(column.to_string(), Value::Int64(value))),
        other => Err(format!("unknown op: {other} (expected eq|lt|lteq|gt|gteq)").into()),
    }
}

fn handle_explain(dir: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
    let column = args.get(3).ok_or("missing <column>")?;
    let op = args.get(4).ok_or("missing <op: eq|lt|lteq|gt|gteq>")?;
    let value: i64 = args.get(5).ok_or("missing <value>")?.parse()?;
    let predicate = parse_predicate(column, op, value)?;

    let ds = strata_txn::Dataset::open(dir)?;
    let result = ds.explain(&predicate);
    println!(
        "total_files={} scanned={} skipped={} predicate={predicate:?}",
        result.total_files,
        result.scanned.len(),
        result.skipped.len(),
    );
    for name in &result.scanned {
        println!("  scan:  {name}");
    }
    for name in &result.skipped {
        println!("  skip:  {name}");
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn unknown_command_errors_even_without_a_dir_argument() {
        let args = vec!["strata".to_string(), "bogus".to_string()];
        let result = run(&args);
        assert!(
            result.is_err(),
            "an unknown command must error, not attempt to run"
        );
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("unknown command"),
            "expected an 'unknown command' error, got: {message}"
        );
    }

    #[test]
    fn resolve_display_rows_maps_row_ids_back_to_display_values() {
        use arrow::array::UInt64Array;
        let row_ids = UInt64Array::from(vec![10, 11, 12]);
        let ids = Int64Array::from(vec![100, 200, 300]);
        let matches = vec![
            strata_index::VectorMatch {
                row_id: 12,
                squared_distance: 1.5,
            },
            strata_index::VectorMatch {
                row_id: 10,
                squared_distance: 2.5,
            },
        ];

        let resolved = resolve_display_rows(&matches, &row_ids, &ids);
        assert_eq!(resolved, vec![(300, 1.5), (100, 2.5)]);
    }

    #[test]
    fn parse_predicate_builds_each_operator_variant() {
        use strata_query::Predicate;
        use strata_storage::Value;

        assert_eq!(
            parse_predicate("id", "eq", 5).unwrap(),
            Predicate::Eq("id".to_string(), Value::Int64(5))
        );
        assert_eq!(
            parse_predicate("id", "lt", 5).unwrap(),
            Predicate::Lt("id".to_string(), Value::Int64(5))
        );
        assert_eq!(
            parse_predicate("id", "lteq", 5).unwrap(),
            Predicate::LtEq("id".to_string(), Value::Int64(5))
        );
        assert_eq!(
            parse_predicate("id", "gt", 5).unwrap(),
            Predicate::Gt("id".to_string(), Value::Int64(5))
        );
        assert_eq!(
            parse_predicate("id", "gteq", 5).unwrap(),
            Predicate::GtEq("id".to_string(), Value::Int64(5))
        );
        assert!(parse_predicate("id", "bogus", 5).is_err());
    }

    #[test]
    fn handle_explain_runs_end_to_end_against_a_real_dataset() {
        let dir =
            std::env::temp_dir().join(format!("strata-cli-explain-test-{}", std::process::id()));
        let dir_str = dir.to_str().unwrap().to_string();
        strata_txn::Dataset::create(&dir_str).unwrap();
        let ds = strata_txn::Dataset::open(&dir_str).unwrap();
        let mut txn = ds.begin();
        txn.insert(make_row(1, "alice", [1.0, 2.0, 3.0]).unwrap());
        txn.commit().unwrap();

        let args = vec![
            "strata".to_string(),
            "explain".to_string(),
            dir_str.clone(),
            "id".to_string(),
            "eq".to_string(),
            "1".to_string(),
        ];
        let result = handle_explain(&dir_str, &args);
        assert!(result.is_ok(), "handle_explain failed: {result:?}");

        std::fs::remove_dir_all(&dir).ok();
    }
}
