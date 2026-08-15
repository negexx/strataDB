#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{Command, Output};
use std::sync::Arc;

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

fn assert_json_value(value: &str) {
    let mut parser = JsonParser {
        input: value.as_bytes(),
        position: 0,
    };
    parser.parse_value();
    parser.skip_whitespace();
    assert_eq!(parser.position, parser.input.len(), "trailing JSON input");
}

struct JsonParser<'a> {
    input: &'a [u8],
    position: usize,
}

impl JsonParser<'_> {
    fn parse_value(&mut self) {
        self.skip_whitespace();
        match self.peek() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'\"') => self.parse_string(),
            Some(b'-' | b'0'..=b'9') => self.parse_number(),
            Some(b't') => self.parse_literal(b"true"),
            Some(b'f') => self.parse_literal(b"false"),
            Some(b'n') => self.parse_literal(b"null"),
            other => panic!("invalid JSON value at {}: {other:?}", self.position),
        }
    }

    fn parse_object(&mut self) {
        self.expect(b'{');
        self.skip_whitespace();
        if self.consume(b'}') {
            return;
        }
        loop {
            self.parse_string();
            self.skip_whitespace();
            self.expect(b':');
            self.parse_value();
            self.skip_whitespace();
            if self.consume(b'}') {
                return;
            }
            self.expect(b',');
            self.skip_whitespace();
        }
    }

    fn parse_array(&mut self) {
        self.expect(b'[');
        self.skip_whitespace();
        if self.consume(b']') {
            return;
        }
        loop {
            self.parse_value();
            self.skip_whitespace();
            if self.consume(b']') {
                return;
            }
            self.expect(b',');
        }
    }

    fn parse_string(&mut self) {
        self.expect(b'\"');
        loop {
            match self.next() {
                Some(b'\"') => return,
                Some(b'\\') => match self.next() {
                    Some(b'\"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => {}
                    Some(b'u') => {
                        for _ in 0..4 {
                            assert!(
                                matches!(
                                    self.next(),
                                    Some(b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')
                                ),
                                "invalid JSON unicode escape"
                            );
                        }
                    }
                    other => panic!("invalid JSON escape: {other:?}"),
                },
                Some(byte) if byte >= 0x20 => {}
                other => panic!("invalid JSON string byte: {other:?}"),
            }
        }
    }

    fn parse_number(&mut self) {
        self.consume(b'-');
        if self.consume(b'0') {
        } else {
            self.expect_range(b'1', b'9');
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.position += 1;
            }
        }
        if self.consume(b'.') {
            self.expect_range(b'0', b'9');
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.position += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.position += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.position += 1;
            }
            self.expect_range(b'0', b'9');
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.position += 1;
            }
        }
    }

    fn parse_literal(&mut self, literal: &[u8]) {
        for byte in literal {
            self.expect(*byte);
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.position += 1;
        }
    }

    fn expect_range(&mut self, lower: u8, upper: u8) {
        assert!(
            matches!(self.next(), Some(value) if (lower..=upper).contains(&value)),
            "expected JSON digit"
        );
    }

    fn expect(&mut self, expected: u8) {
        assert_eq!(
            self.next(),
            Some(expected),
            "expected JSON byte {expected:?}"
        );
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.position).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let value = self.peek();
        self.position += usize::from(value.is_some());
        value
    }
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
    assert_json_value(&stdout(&output));
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
fn explain_json_reports_the_planned_operator_path() {
    // Break caught: CLI explain reports only legacy file pruning rather than
    // the stable logical and physical operators selected for a typed scan.
    let dir = empty_fixture_dir();
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
        "{\"kind\":\"explain\",\"logical_operators\":[\"source\",\"predicate\",\"projection\",\"materialize\"],\"physical_operators\":[\"manifest_snapshot_source\",\"zone_map_pruning\",\"tombstone_filter\",\"row_filter\",\"column_projection\",\"materialize\"],\"observations\":{\"data_files_total\":0,\"data_files_scanned\":0,\"data_files_pruned\":0,\"index_segments_total\":0,\"index_segments_scanned\":0,\"index_segments_pruned\":0,\"transaction_overlay\":false}}\n"
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
    // Break caught: operators have no stable discovery path for the admin
    // commands or mistake a CLI timing command for the retained Criterion
    // evidence fixture.
    let help = command(&["help"]);
    assert!(help.status.success(), "help failed: {}", stderr(&help));
    assert_eq!(
        stdout(&help),
        "usage: strata <create|insert|scan|filter|search|explain|inspect|schema|migration|manifest-status|recovery-status|evidence|crash-loop|lookup|group-by|query-scan> <dir> [...]\n"
    );

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
        "--ack-single-writer",
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
    assert_json_value(&stdout(&output));
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
