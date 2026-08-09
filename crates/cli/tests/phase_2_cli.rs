#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{
    FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};

fn command(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-phase-2-")
        .tempdir()
        .unwrap();
    let dir_str = dir.path().to_str().unwrap();

    let created = command(&["create", dir_str, "--ack-single-writer"]);
    assert!(
        created.status.success(),
        "create failed: {}",
        stderr(&created)
    );
    for (id, name, vector) in [
        ("1", "alice", ["1.0", "2.0", "3.0"]),
        ("2", "bob", ["4.0", "5.0", "6.0"]),
    ] {
        let inserted = command(&[
            "insert",
            dir_str,
            id,
            name,
            vector[0],
            vector[1],
            vector[2],
            "--ack-single-writer",
        ]);
        assert!(
            inserted.status.success(),
            "insert failed: {}",
            stderr(&inserted)
        );
    }
    dir
}

fn typed_fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-typed-")
        .tempdir()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ]));
    let dataset = strata_txn::Dataset::create(dir.path(), schema.clone()).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
            Arc::new(Float64Array::from(vec![Some(1.5), None])),
        ],
    )
    .unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
    dir
}

fn typed_vector_fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-typed-vector-")
        .tempdir()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
            false,
        ),
    ]));
    let vectors = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        3,
        Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 2.0, 3.0, 4.0])),
        None,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![0.5, 1.5])),
            Arc::new(vectors),
        ],
    )
    .unwrap();
    let dataset = strata_txn::Dataset::create(dir.path(), schema).unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
    dir
}

fn two_dimensional_vector_fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-2d-vector-")
        .tempdir()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
            false,
        ),
    ]));
    let vectors = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        2,
        Arc::new(Float32Array::from(vec![1.0, 2.0, 4.0, 5.0])),
        None,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(vectors)],
    )
    .unwrap();
    let dataset = strata_txn::Dataset::create(dir.path(), schema).unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
    dir
}

fn vector_only_fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-vector-only-")
        .tempdir()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
        false,
    )]));
    let vectors = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        2,
        Arc::new(Float32Array::from(vec![1.0, 2.0, 4.0, 5.0])),
        None,
    );
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors)]).unwrap();
    let dataset = strata_txn::Dataset::create(dir.path(), schema).unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
    dir
}

#[test]
fn create_requires_an_explicit_single_writer_acknowledgement() {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-missing-ack-")
        .tempdir()
        .unwrap();
    let output = command(&["create", dir.path().to_str().unwrap()]);

    assert!(
        !output.status.success(),
        "create must reject a missing acknowledgement"
    );
    assert_eq!(
        stderr(&output),
        "error: usage error: create requires --ack-single-writer; this acknowledges only one process using one shared Dataset handle, not cross-process coordination or serialization\n"
    );

    let insert = command(&[
        "insert",
        dir.path().to_str().unwrap(),
        "1",
        "alice",
        "1.0",
        "2.0",
        "3.0",
    ]);
    assert!(
        !insert.status.success(),
        "insert must reject a missing acknowledgement"
    );
    assert_eq!(
        stderr(&insert),
        "error: usage error: insert requires --ack-single-writer; this acknowledges only one process using one shared Dataset handle, not cross-process coordination or serialization\n"
    );

    let crash_loop = command(&["crash-loop", dir.path().to_str().unwrap(), "1"]);
    assert!(
        !crash_loop.status.success(),
        "crash-loop must reject a missing acknowledgement"
    );
    assert_eq!(
        stderr(&crash_loop),
        "error: usage error: crash-loop requires --ack-single-writer; this acknowledges only one process using one shared Dataset handle, not cross-process coordination or serialization\n"
    );
}

#[test]
fn generic_query_commands_render_stable_typed_lines() {
    let dir = fixture_dir();
    let dir_str = dir.path().to_str().unwrap();

    let lookup = command(&["lookup", dir_str, "0", "--columns", "id,name"]);
    assert!(
        lookup.status.success(),
        "lookup failed: {}",
        stderr(&lookup)
    );
    assert_eq!(
        stdout(&lookup),
        concat!(
            "lookup row_id=0 outcome=live\n",
            "field name=id value=Int64(1)\n",
            "field name=name value=Utf8(\"alice\")\n",
        )
    );

    let scan = command(&[
        "query-scan",
        dir_str,
        "--columns",
        "id,name",
        "--filter",
        "id",
        "gt",
        "1",
    ]);
    assert!(
        scan.status.success(),
        "query-scan failed: {}",
        stderr(&scan)
    );
    assert_eq!(
        stdout(&scan),
        concat!(
            "query-scan projection=id,name\n",
            "row index=0 id=Int64(2) name=Utf8(\"bob\")\n",
        )
    );

    let grouped = command(&["group-by", dir_str, "name", "--agg", "avg:id"]);
    assert!(
        grouped.status.success(),
        "group-by failed: {}",
        stderr(&grouped)
    );
    assert_eq!(
        stdout(&grouped),
        concat!(
            "group-by keys=name aggregates=avg_id:Float64\n",
            "group name=Utf8(\"alice\") avg_id=Float64(1)\n",
            "group name=Utf8(\"bob\") avg_id=Float64(2)\n",
        )
    );

    let counted = command(&["group-by", dir_str, "name", "--agg", "count:id"]);
    assert!(
        counted.status.success(),
        "count group-by failed: {}",
        stderr(&counted)
    );
    assert_eq!(
        stdout(&counted),
        concat!(
            "group-by keys=name aggregates=count_id:UInt64\n",
            "group name=Utf8(\"alice\") count_id=UInt64(1)\n",
            "group name=Utf8(\"bob\") count_id=UInt64(1)\n",
        )
    );

    let summed = command(&["group-by", dir_str, "name", "--agg", "sum:id"]);
    assert!(
        summed.status.success(),
        "sum group-by failed: {}",
        stderr(&summed)
    );
    assert_eq!(
        stdout(&summed),
        concat!(
            "group-by keys=name aggregates=sum_id:Int64\n",
            "group name=Utf8(\"alice\") sum_id=Int64(1)\n",
            "group name=Utf8(\"bob\") sum_id=Int64(2)\n",
        )
    );
}

#[test]
fn generic_query_errors_identify_their_validation_kind() {
    let dir = fixture_dir();
    let output = command(&[
        "query-scan",
        dir.path().to_str().unwrap(),
        "--columns",
        "not_a_column",
    ]);

    assert!(!output.status.success(), "invalid projection must fail");
    assert_eq!(
        stderr(&output),
        "error: query error kind=validation message=unknown dataset column 'not_a_column'\n"
    );
}

#[test]
fn insert_accepts_acknowledgement_before_payload_and_reports_typed_usage_errors() {
    let dir = fixture_dir();
    let dir_str = dir.path().to_str().unwrap();
    let output = command(&[
        "insert",
        dir_str,
        "--ack-single-writer",
        "3",
        "carol",
        "7.0",
        "8.0",
        "9.0",
    ]);
    assert!(
        output.status.success(),
        "insert failed: {}",
        stderr(&output)
    );

    let malformed = command(&[
        "insert",
        dir_str,
        "--ack-single-writer",
        "not-an-id",
        "carol",
        "7.0",
        "8.0",
        "9.0",
    ]);
    assert!(!malformed.status.success());
    assert_eq!(
        stderr(&malformed),
        "error: usage error: insert <id> must be an Int64\n"
    );
}

#[test]
fn query_filters_use_the_opened_dataset_schema_and_render_nulls() {
    let dir = typed_fixture_dir();
    let dir_str = dir.path().to_str().unwrap();

    let scan = command(&[
        "query-scan",
        dir_str,
        "--columns",
        "name,score",
        "--filter",
        "score",
        "gt",
        "1.0",
    ]);
    assert!(
        scan.status.success(),
        "query-scan failed: {}",
        stderr(&scan)
    );
    assert_eq!(
        stdout(&scan),
        concat!(
            "query-scan projection=name,score\n",
            "row index=0 name=Utf8(\"alice\") score=Float64(1.5)\n",
        )
    );

    let grouped = command(&["group-by", dir_str, "name", "--agg", "avg:score"]);
    assert!(
        grouped.status.success(),
        "group-by failed: {}",
        stderr(&grouped)
    );
    assert_eq!(
        stdout(&grouped),
        concat!(
            "group-by keys=name aggregates=avg_score:Float64\n",
            "group name=Utf8(\"alice\") avg_score=Float64(1.5)\n",
            "group name=Utf8(\"bob\") avg_score=Null\n",
        )
    );
}

#[test]
fn lookup_reports_tombstoned_and_not_found_outcomes() {
    let dir = fixture_dir();
    let dataset = strata_txn::Dataset::open(dir.path()).unwrap();
    let mut transaction = dataset.begin();
    transaction.delete(0).unwrap();
    transaction.commit().unwrap();
    let dir_str = dir.path().to_str().unwrap();

    let tombstoned = command(&["lookup", dir_str, "0"]);
    assert!(
        tombstoned.status.success(),
        "lookup failed: {}",
        stderr(&tombstoned)
    );
    assert_eq!(stdout(&tombstoned), "lookup row_id=0 outcome=tombstoned\n");

    let not_found = command(&["lookup", dir_str, "999"]);
    assert!(
        not_found.status.success(),
        "lookup failed: {}",
        stderr(&not_found)
    );
    assert_eq!(stdout(&not_found), "lookup row_id=999 outcome=not_found\n");
}

#[test]
fn search_reports_unresolved_vector_matches_as_typed_errors() {
    let dir = fixture_dir();
    let output = command(&[
        "search",
        dir.path().to_str().unwrap(),
        "1.0",
        "2.0",
        "3.0",
        "1",
    ]);
    assert!(
        output.status.success(),
        "search failed: {}",
        stderr(&output)
    );
}

#[test]
fn search_uses_persisted_schema_vector_query_and_typed_filtering() {
    let dir = typed_vector_fixture_dir();
    let output = command(&[
        "search",
        dir.path().to_str().unwrap(),
        "1.0",
        "2.0",
        "3.0",
        "2",
        "--filter",
        "score",
        "gt",
        "1.0",
    ]);

    assert!(
        output.status.success(),
        "search failed: {}",
        stderr(&output)
    );
    assert_eq!(stdout(&output), "row_id=1 squared_distance=3\n");
}

#[test]
fn search_exact_is_rejected_with_a_typed_usage_error() {
    let dir = fixture_dir();
    let output = command(&[
        "search",
        dir.path().to_str().unwrap(),
        "1.0",
        "2.0",
        "3.0",
        "1",
        "--exact",
    ]);

    assert!(!output.status.success());
    assert_eq!(
        stderr(&output),
        "error: usage error: search --exact is not supported; use the typed vector-search contract\n"
    );
}

#[test]
fn search_accepts_comma_separated_vectors_for_persisted_dimensions() {
    let dir = two_dimensional_vector_fixture_dir();
    let output = command(&[
        "search",
        dir.path().to_str().unwrap(),
        "--vector",
        "1.0,2.0",
        "--k",
        "1",
    ]);

    assert!(
        output.status.success(),
        "search failed: {}",
        stderr(&output)
    );
    assert_eq!(stdout(&output), "row_id=0 squared_distance=0\n");
}

#[test]
fn search_supports_vector_only_datasets_with_physical_row_ids() {
    let dir = vector_only_fixture_dir();
    let output = command(&[
        "search",
        dir.path().to_str().unwrap(),
        "--vector",
        "1.0,2.0",
        "--k",
        "1",
    ]);

    assert!(
        output.status.success(),
        "search failed: {}",
        stderr(&output)
    );
    assert_eq!(stdout(&output), "row_id=0 squared_distance=0\n");
}

#[test]
fn search_rejects_invalid_comma_separated_vector_with_typed_usage_error() {
    let dir = two_dimensional_vector_fixture_dir();
    let output = command(&[
        "search",
        dir.path().to_str().unwrap(),
        "--vector",
        "1.0,NaN",
    ]);

    assert!(!output.status.success());
    assert_eq!(
        stderr(&output),
        "error: usage error: --vector must contain finite comma-separated floats\n"
    );
}
