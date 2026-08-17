#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{Command, Output};
use std::sync::Arc;

use arrow::array::{
    BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array, StringArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::{Value, json};

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

fn assert_json_value(value: &str) {
    serde_json::from_str::<Value>(value)
        .unwrap_or_else(|error| panic!("expected JSON stdout: {error}; stdout was: {value}"));
}

fn json_output(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "expected JSON stdout: {error}; stdout was: {}",
            stdout(output)
        )
    })
}

fn json_error_output(output: &Output) -> Value {
    serde_json::from_slice(&output.stderr).unwrap_or_else(|error| {
        panic!(
            "expected JSON stderr: {error}; stderr was: {}",
            stderr(output)
        )
    })
}

fn empty_fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-admin-")
        .tempdir()
        .unwrap();
    let dir_str = dir.path().to_str().unwrap();
    let created = command(&["create", dir_str, "--ack-single-writer"]);
    assert!(
        created.status.success(),
        "create failed: {}",
        stderr(&created)
    );
    dir
}

fn quoted_backslashed_schema_fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-admin-quoted-schema-")
        .tempdir()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "quoted\"\\column",
        DataType::Utf8,
        true,
    )]));
    strata_txn::Dataset::create(dir.path().to_str().unwrap(), schema).unwrap();
    dir
}

fn vector_fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-admin-vector-")
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
    let dataset = strata_txn::Dataset::create(dir.path(), Arc::clone(&schema)).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(FixedSizeListArray::new(
                Arc::new(Field::new("item", DataType::Float32, false)),
                2,
                Arc::new(Float32Array::from(vec![0.0, 0.0])),
                None,
            )),
        ],
    )
    .unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
    dir
}

fn lookup_json_fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-admin-lookup-json-")
        .tempdir()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let dataset = strata_txn::Dataset::create(dir.path(), Arc::clone(&schema)).unwrap();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
    dir
}

fn lookup_json_value_fixture_dir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("strata-cli-admin-lookup-json-values-")
        .tempdir()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("flag", DataType::Boolean, false),
        Field::new("count", DataType::UInt64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
            false,
        ),
        Field::new("missing_score", DataType::Float64, true),
    ]));
    let dataset = strata_txn::Dataset::create(dir.path(), Arc::clone(&schema)).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(UInt64Array::from(vec![42])),
            Arc::new(Float64Array::from(vec![1.5])),
            Arc::new(StringArray::from(vec!["quote\"\\line\nend"])),
            Arc::new(FixedSizeListArray::new(
                Arc::new(Field::new("item", DataType::Float32, false)),
                2,
                Arc::new(Float32Array::from(vec![0.25, 0.75])),
                None,
            )),
            Arc::new(Float64Array::from(vec![None])),
        ],
    )
    .unwrap();
    let mut transaction = dataset.begin();
    transaction.insert(batch).unwrap();
    transaction.commit().unwrap();
    dir
}

#[test]
fn schema_json_reports_the_persisted_catalog_and_fields() {
    // Break caught: CLI schema inspection either assumes the legacy fixture
    // without opening the dataset or omits the persisted catalog version.
    let dir = empty_fixture_dir();
    let output = command(&["schema", dir.path().to_str().unwrap(), "--json"]);

    assert!(
        output.status.success(),
        "schema failed: {}",
        stderr(&output)
    );
    assert_json_value(&stdout(&output));
    assert_eq!(
        stdout(&output),
        concat!(
            "{\"kind\":\"schema\",\"manifest_version\":0,\"schema_version\":1,\"fields\":[",
            "{\"name\":\"id\",\"type\":\"int64\",\"nullable\":false},",
            "{\"name\":\"name\",\"type\":\"utf8\",\"nullable\":false},",
            "{\"name\":\"vector\",\"type\":\"vector(3)\",\"nullable\":false}]}",
            "\n"
        )
    );
}

#[test]
fn schema_json_escapes_quoted_and_backslashed_column_names() {
    // Break caught: schema JSON becomes unparsable when a persisted column
    // name contains a quote or backslash.
    let dir = quoted_backslashed_schema_fixture_dir();
    let output = command(&["schema", dir.path().to_str().unwrap(), "--json"]);

    assert!(
        output.status.success(),
        "schema failed: {}",
        stderr(&output)
    );
    assert_eq!(
        json_output(&output),
        json!({
            "kind": "schema",
            "manifest_version": 0,
            "schema_version": 1,
            "fields": [{
                "name": "quoted\"\\column",
                "type": "utf8",
                "nullable": true,
            }],
        })
    );
}

#[test]
fn inspect_json_reports_the_captured_schema_and_row_count() {
    // Break caught: inspect's JSON mode either reads the legacy fixture
    // schema or loses the catalog version bound to the opened dataset.
    let dir = empty_fixture_dir();
    let output = command(&["inspect", dir.path().to_str().unwrap(), "--json"]);

    assert!(
        output.status.success(),
        "inspect failed: {}",
        stderr(&output)
    );
    assert_json_value(&stdout(&output));
    assert_eq!(
        stdout(&output),
        "{\"kind\":\"inspect\",\"manifest_version\":0,\"schema_version\":1,\"row_count\":0}\n"
    );
}

#[test]
fn manifest_and_recovery_status_are_json_after_a_reopen() {
    // Break caught: a status command either reports an unanchored directory
    // listing or emits non-JSON after recovery selected a manifest.
    let dir = empty_fixture_dir();
    let dir_str = dir.path().to_str().unwrap();

    let manifest = command(&["manifest-status", dir_str, "--json"]);
    assert!(
        manifest.status.success(),
        "manifest status failed: {}",
        stderr(&manifest)
    );
    assert_json_value(&stdout(&manifest));
    assert_eq!(
        stdout(&manifest),
        "{\"kind\":\"manifest_status\",\"observed_version\":0,\"schema_version\":1,\"manifest_object_count\":1,\"current_manifest_present\":true,\"data_object_count\":0,\"reachable_data_file_count\":0,\"reachable_segment_count\":0,\"orphan_candidate_count\":0}\n"
    );

    let recovery = command(&["recovery-status", dir_str, "--json"]);
    assert!(
        recovery.status.success(),
        "recovery status failed: {}",
        stderr(&recovery)
    );
    assert_json_value(&stdout(&recovery));
    assert_eq!(
        stdout(&recovery),
        "{\"kind\":\"recovery_status\",\"manifest_version\":0,\"schema_version\":1,\"physical_row_count\":0,\"tombstone_count\":0}\n"
    );
}

#[test]
fn explain_json_serializes_scalar_zero_segment_counts() {
    // Break caught: CLI explain serializes segment scans for a scalar physical
    // plan even though the plan reads only manifest-listed row files.
    let dir = vector_fixture_dir();
    let output = command(&[
        "explain",
        dir.path().to_str().unwrap(),
        "id",
        "eq",
        "1",
        "--json",
    ]);

    assert!(
        output.status.success(),
        "explain failed: {}",
        stderr(&output)
    );
    assert_json_value(&stdout(&output));
    assert_eq!(
        stdout(&output),
        "{\"kind\":\"explain\",\"logical_operators\":[\"source\",\"predicate\",\"projection\",\"materialize\"],\"physical_operators\":[\"manifest_snapshot_source\",\"zone_map_pruning\",\"tombstone_filter\",\"row_filter\",\"column_projection\",\"materialize\"],\"observations\":{\"data_files_total\":1,\"data_files_scanned\":1,\"data_files_pruned\":0,\"index_segments_total\":1,\"index_segments_scanned\":0,\"index_segments_pruned\":0,\"transaction_overlay\":false}}\n"
    );
}

#[test]
fn explain_accepts_the_readme_predicate_form_with_json_output() {
    // Break caught: the README-compatible predicate-bearing invocation either
    // fails to parse or stops producing a structured explain result.
    let dir = vector_fixture_dir();
    let output = command(&[
        "explain",
        dir.path().to_str().unwrap(),
        "id",
        "eq",
        "1",
        "--json",
    ]);

    assert!(
        output.status.success(),
        "README-compatible explain command failed: {}",
        stderr(&output)
    );
    let explanation = json_output(&output);
    assert_eq!(explanation["kind"], "explain");
    assert!(
        explanation["logical_operators"]
            .as_array()
            .is_some_and(|operators| operators.contains(&json!("predicate"))),
        "explain JSON must record the parsed predicate: {explanation}"
    );
}

#[test]
fn admin_errors_use_distinct_stable_exit_categories() {
    // Break caught: automation cannot distinguish a bad invocation from an
    // unsupported migration, corrupt durable state, or an operational open
    // failure by the process exit category.
    let dir = empty_fixture_dir();
    let dir_str = dir.path().to_str().unwrap();

    let usage = command(&["schema", dir_str, "--not-json"]);
    assert_eq!(usage.status.code(), Some(2));

    let first_migration = command(&[
        "migration",
        "run",
        dir_str,
        "add-nullable-column",
        "tag",
        "utf8",
        "--ack-single-writer",
    ]);
    assert!(
        first_migration.status.success(),
        "setup migration failed: {}",
        stderr(&first_migration)
    );
    let unsupported = command(&[
        "migration",
        "run",
        dir_str,
        "add-nullable-column",
        "next_tag",
        "utf8",
        "--ack-single-writer",
    ]);
    assert_eq!(unsupported.status.code(), Some(4));

    let corrupt_dir = empty_fixture_dir();
    std::fs::write(
        corrupt_dir
            .path()
            .join("_versions")
            .join("00000000000000000000.manifest"),
        "not JSON",
    )
    .unwrap();
    let corruption = command(&["schema", corrupt_dir.path().to_str().unwrap()]);
    assert_eq!(corruption.status.code(), Some(5));

    let missing = tempfile::Builder::new()
        .prefix("strata-cli-admin-missing-")
        .tempdir()
        .unwrap();
    let operational = command(&["schema", missing.path().to_str().unwrap()]);
    assert_eq!(operational.status.code(), Some(1));
}

#[test]
fn missing_dataset_argument_is_a_usage_error() {
    // Break caught: an omitted dataset argument is reported as an operational
    // failure even though the invocation is invalid before opening a dataset.
    let output = command(&["schema"]);

    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        stderr(&output),
        "error: usage error: missing <dir> argument\n"
    );
}

#[test]
fn explain_rejects_missing_and_malformed_grammar_before_opening_a_dataset() {
    // Break caught: explain opens the supplied path before validating its
    // complete grammar, causing malformed invocations to report operational
    // errors instead of the stable usage category.
    let missing_dir = tempfile::tempdir().unwrap();
    let missing_dir = missing_dir.path().join("missing");
    let missing_dir = missing_dir.to_str().unwrap();
    let cases = [
        vec!["explain", "--json"],
        vec!["explain", missing_dir, "id", "--json"],
        vec!["explain", missing_dir, "id", "bogus", "1", "--json"],
        vec![
            "explain",
            missing_dir,
            "id",
            "eq",
            "1",
            "--unknown",
            "--json",
        ],
    ];

    for args in cases {
        let output = command(&args);
        assert_eq!(output.status.code(), Some(2), "args: {args:?}");
        let error: Value = serde_json::from_str(&stderr(&output))
            .unwrap_or_else(|error| panic!("expected JSON error: {error}; args: {args:?}"));
        assert_eq!(error["error"]["category"], "usage", "args: {args:?}");
    }
}

#[test]
fn inspect_rejects_unknown_trailing_options() {
    // Break caught: inspect silently accepts an unsupported trailing flag and
    // emits a non-JSON inspection result.
    let dir = empty_fixture_dir();
    let output = command(&["inspect", dir.path().to_str().unwrap(), "--unknown"]);

    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        stderr(&output),
        "error: usage error: inspect accepts only an optional --json flag\n"
    );
}

#[test]
fn help_and_evidence_commands_describe_the_supported_operational_surface() {
    // Break caught: CLI help omits executable forms for predicate, lookup
    // JSON, or typed query-scan syntax.
    let help = command(&["help"]);
    assert!(help.status.success(), "help failed: {}", stderr(&help));
    let help_output = stdout(&help);
    for required_line in [
        "  explain <dir> <column> <op> <value> [--json]",
        "  lookup <dir> <row_id> [--columns <column,...>] [--json]",
        "  query-scan <dir> --columns <column,...> [--filter <column> <op> <value>]",
    ] {
        assert!(
            help_output.lines().any(|line| line == required_line),
            "help must document `{required_line}`; help output was: {help_output}"
        );
    }

    let bare = command(&[]);
    assert!(
        bare.status.success(),
        "bare command failed: {}",
        stderr(&bare)
    );
    assert_eq!(
        stderr(&bare).lines().next(),
        stdout(&help).lines().next(),
        "bare usage must expose the same command list as help"
    );

    let evidence = command(&["evidence", "--json"]);
    assert!(
        evidence.status.success(),
        "evidence failed: {}",
        stderr(&evidence)
    );
    assert_json_value(&stdout(&evidence));
    assert_eq!(
        stdout(&evidence),
        "{\"kind\":\"evidence\",\"criterion_command\":\"cargo bench -p strata-bench --bench query_planner_bench\",\"report\":\"docs/phase-3-verification-report.md#task-3-query-planning-evidence\"}\n"
    );
}

#[test]
fn lookup_json_returns_a_stable_envelope_for_live_tombstoned_and_not_found_rows() {
    // Break caught: lookup's JSON mode is missing or changes the stable
    // envelope. Every response carries kind, row_id, outcome, and fields;
    // fields is an array and is empty when no live row can be projected.
    let dir = lookup_json_fixture_dir();
    let dir_str = dir.path().to_str().unwrap();

    let live = command(&["lookup", dir_str, "0", "--json"]);
    assert!(
        live.status.success(),
        "live lookup failed: {}",
        stderr(&live)
    );
    assert_eq!(
        json_output(&live),
        json!({
            "kind": "lookup",
            "row_id": 0,
            "outcome": "live",
            "fields": [{
                "name": "id",
                "value": { "type": "int64", "value": 1 },
            }],
        })
    );

    let dataset = strata_txn::Dataset::open(dir.path()).unwrap();
    let mut transaction = dataset.begin();
    transaction.delete(0).unwrap();
    transaction.commit().unwrap();

    let tombstoned = command(&["lookup", dir_str, "0", "--json"]);
    assert!(
        tombstoned.status.success(),
        "tombstoned lookup failed: {}",
        stderr(&tombstoned)
    );
    assert_eq!(
        json_output(&tombstoned),
        json!({
            "kind": "lookup",
            "row_id": 0,
            "outcome": "tombstoned",
            "fields": [],
        })
    );

    let not_found = command(&["lookup", dir_str, "999", "--json"]);
    assert!(
        not_found.status.success(),
        "not-found lookup failed: {}",
        stderr(&not_found)
    );
    assert_eq!(
        json_output(&not_found),
        json!({
            "kind": "lookup",
            "row_id": 999,
            "outcome": "not_found",
            "fields": [],
        })
    );
}

#[test]
fn lookup_rejects_json_as_a_missing_columns_value() {
    // Break caught: `--json` is consumed as a projection name, so malformed
    // syntax reaches query validation and loses the stable usage category.
    let dir = lookup_json_fixture_dir();
    let output = command(&[
        "lookup",
        dir.path().to_str().unwrap(),
        "0",
        "--columns",
        "--json",
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert_eq!(json_error_output(&output)["error"]["category"], "usage");
}

#[test]
fn lookup_json_preserves_typed_live_values() {
    // Break caught: values lose their logical types, strings are not escaped,
    // vectors are flattened, or nullable values stop using a JSON null.
    let dir = lookup_json_value_fixture_dir();
    let output = command(&["lookup", dir.path().to_str().unwrap(), "0", "--json"]);

    assert!(
        output.status.success(),
        "lookup failed: {}",
        stderr(&output)
    );
    assert_eq!(
        json_output(&output),
        json!({
            "kind": "lookup",
            "row_id": 0,
            "outcome": "live",
            "fields": [
                { "name": "flag", "value": { "type": "boolean", "value": true } },
                { "name": "count", "value": { "type": "uint64", "value": 42 } },
                { "name": "score", "value": { "type": "float64", "value": 1.5 } },
                { "name": "label", "value": { "type": "utf8", "value": "quote\"\\line\nend" } },
                { "name": "embedding", "value": { "type": "vector", "value": [0.25, 0.75] } },
                { "name": "missing_score", "value": { "type": "null", "value": null } },
            ],
        })
    );
}

#[test]
fn migration_validate_run_and_status_publish_the_explicit_catalog_transition() {
    // Break caught: the CLI either accepts an implicit schema rewrite or
    // reports stale catalog/manifest details after a durable migration.
    let dir = empty_fixture_dir();
    let dir_str = dir.path().to_str().unwrap();

    let validated = command(&[
        "migration",
        "validate",
        dir_str,
        "add-nullable-column",
        "tag",
        "utf8",
        "--json",
    ]);
    assert!(
        validated.status.success(),
        "migration validate failed: {}",
        stderr(&validated)
    );
    assert_json_value(&stdout(&validated));
    assert_eq!(
        stdout(&validated),
        "{\"kind\":\"migration_validation\",\"name\":\"add_nullable_column\",\"source_schema_version\":1,\"target_schema_version\":2,\"column\":{\"name\":\"tag\",\"type\":\"utf8\",\"nullable\":true}}\n"
    );

    let run = command(&[
        "migration",
        "run",
        dir_str,
        "add-nullable-column",
        "tag",
        "utf8",
        "--ack-single-writer",
        "--json",
    ]);
    assert!(
        run.status.success(),
        "migration run failed: {}",
        stderr(&run)
    );
    assert_json_value(&stdout(&run));
    assert_eq!(
        stdout(&run),
        "{\"kind\":\"migration_result\",\"name\":\"add_nullable_column\",\"source_schema_version\":1,\"target_schema_version\":2,\"manifest_version\":1}\n"
    );

    let status = command(&["migration", "status", dir_str, "--json"]);
    assert!(
        status.status.success(),
        "migration status failed: {}",
        stderr(&status)
    );
    assert_json_value(&stdout(&status));
    assert_eq!(
        stdout(&status),
        "{\"kind\":\"migration_status\",\"manifest_version\":1,\"schema_version\":2}\n"
    );
}

#[test]
fn migration_validation_json_escapes_quoted_and_backslashed_column_names() {
    // Break caught: migration validation JSON becomes unparsable when the
    // requested nullable column name contains a quote or backslash.
    let dir = empty_fixture_dir();
    let output = command(&[
        "migration",
        "validate",
        dir.path().to_str().unwrap(),
        "add-nullable-column",
        "quoted\"\\column",
        "utf8",
        "--json",
    ]);

    assert!(
        output.status.success(),
        "migration validation failed: {}",
        stderr(&output)
    );
    assert_eq!(
        json_output(&output),
        json!({
            "kind": "migration_validation",
            "name": "add_nullable_column",
            "source_schema_version": 1,
            "target_schema_version": 2,
            "column": {
                "name": "quoted\"\\column",
                "type": "utf8",
                "nullable": true,
            },
        })
    );
}

#[test]
fn migration_run_requires_and_accepts_single_writer_acknowledgement() {
    // Break caught: migration execution mutates a dataset without the same
    // single-writer acknowledgement required by other mutation commands.
    let dir = empty_fixture_dir();
    let dir_str = dir.path().to_str().unwrap();

    let rejected = command(&[
        "migration",
        "run",
        dir_str,
        "add-nullable-column",
        "tag",
        "utf8",
    ]);
    assert_eq!(rejected.status.code(), Some(2));
    assert_eq!(
        stderr(&rejected),
        "error: usage error: migration run requires --ack-single-writer; this acknowledges only one process using one shared Dataset handle, not cross-process coordination or serialization\n"
    );

    let accepted = command(&[
        "migration",
        "run",
        dir_str,
        "add-nullable-column",
        "tag",
        "utf8",
        "--ack-single-writer",
    ]);
    assert!(
        accepted.status.success(),
        "acknowledged migration run failed: {}",
        stderr(&accepted)
    );
}

#[test]
fn migration_reserved_schema_names_have_validate_run_usage_parity_without_writes() {
    // Break caught: validate accepted a schema that run rejected later, or a
    // rejected request created replacement objects before returning.
    for reserved_name in ["_row_id", "_timestamp"] {
        let dir = empty_fixture_dir();
        let dir_str = dir.path().to_str().unwrap();
        let manifest_before = std::fs::read(
            dir.path()
                .join("_versions")
                .join("00000000000000000000.manifest"),
        )
        .unwrap();
        let mut objects_before = std::fs::read_dir(dir.path().join("data"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        objects_before.sort();

        let validated = command(&[
            "migration",
            "validate",
            dir_str,
            "add-nullable-column",
            reserved_name,
            "utf8",
        ]);
        let run = command(&[
            "migration",
            "run",
            dir_str,
            "add-nullable-column",
            reserved_name,
            "utf8",
            "--ack-single-writer",
        ]);

        assert_eq!(validated.status.code(), Some(2), "{reserved_name}");
        assert_eq!(run.status.code(), Some(2), "{reserved_name}");
        assert_eq!(stderr(&validated), stderr(&run), "{reserved_name}");
        assert_eq!(
            std::fs::read(
                dir.path()
                    .join("_versions")
                    .join("00000000000000000000.manifest"),
            )
            .unwrap(),
            manifest_before,
            "{reserved_name} must not rewrite the current manifest"
        );
        let mut objects_after = std::fs::read_dir(dir.path().join("data"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        objects_after.sort();
        assert_eq!(
            objects_after, objects_before,
            "{reserved_name} must not create replacement objects"
        );
    }
}

#[test]
fn migration_rejects_unknown_action_before_opening_the_dataset() {
    // Break caught: malformed migration actions open their supplied path and
    // report an operational filesystem error instead of CLI usage.
    let missing_dir = tempfile::tempdir().unwrap();
    let output = command(&[
        "migration",
        "bogus",
        missing_dir.path().join("missing").to_str().unwrap(),
    ]);

    assert_eq!(output.status.code(), Some(2));
    assert_eq!(
        stderr(&output),
        "error: usage error: migration requires validate, run, or status as its first argument\n"
    );
}
