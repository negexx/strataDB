//! `strata` CLI — dataset/manifest inspection and typed query surfaces. The
//! legacy checklist commands remain compatibility coverage. `crash-loop`
//! exists specifically to be killed mid-write by
//! `crates/cli/tests/mvp_checklist_6_crash_recovery.rs`'s crash-recovery
//! test (checklist step 6): it commits one row at a time, printing (and
//! flushing) "committed N" after each success, so an external harness can
//! kill it deterministically partway through and verify recovery.

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Schema};
use std::env;
use std::error::Error;
use std::fmt;
use std::io::Write as _;
use std::process::ExitCode;

const ACK_SINGLE_WRITER: &str = "--ack-single-writer";
const SINGLE_WRITER_BOUNDARY: &str = "this acknowledges only one process using one shared Dataset handle, not cross-process coordination or serialization";

#[derive(Debug)]
enum CliError {
    Usage(String),
    UnknownCommand(String),
    Query { kind: &'static str, message: String },
    VectorResolution(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) => write!(formatter, "usage error: {message}"),
            Self::UnknownCommand(command) => write!(formatter, "unknown command: {command}"),
            Self::Query { kind, message } => {
                write!(formatter, "query error kind={kind} message={message}")
            }
            Self::VectorResolution(message) => write!(formatter, "vector search error: {message}"),
        }
    }
}

impl Error for CliError {}

#[derive(Clone, Copy)]
enum ExitCategory {
    Operational,
    Usage,
    Conflict,
    Unsupported,
    Corruption,
}

impl ExitCategory {
    const fn code(self) -> u8 {
        match self {
            Self::Operational => 1,
            Self::Usage => 2,
            Self::Conflict => 3,
            Self::Unsupported => 4,
            Self::Corruption => 5,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Operational => "operational",
            Self::Usage => "usage",
            Self::Conflict => "conflict",
            Self::Unsupported => "unsupported",
            Self::Corruption => "corruption",
        }
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            let category = error_category(e.as_ref());
            if args.iter().any(|argument| argument == "--json") {
                eprintln!(
                    "{{\"error\":{{\"category\":\"{}\",\"message\":{}}}}}",
                    category.name(),
                    json_string(&e.to_string()),
                );
            } else {
                eprintln!("error: {e}");
            }
            ExitCode::from(category.code())
        }
    }
}

fn error_category(error: &(dyn Error + 'static)) -> ExitCategory {
    let mut current = Some(error);
    while let Some(error) = current {
        if matches!(
            error.downcast_ref::<CliError>(),
            Some(CliError::Usage(_) | CliError::UnknownCommand(_))
        ) {
            return ExitCategory::Usage;
        }
        if let Some(error) = error.downcast_ref::<strata_txn::TxnError>() {
            match error {
                strata_txn::TxnError::Conflict { .. } => return ExitCategory::Conflict,
                strata_txn::TxnError::CorruptSegment(_)
                | strata_txn::TxnError::UnsafeManifestPath(_) => {
                    return ExitCategory::Corruption;
                }
                strata_txn::TxnError::ReservedColumnName(_) => return ExitCategory::Usage,
                strata_txn::TxnError::Storage(error) => return storage_error_category(error),
                _ => {}
            }
        }
        if let Some(error) = error.downcast_ref::<strata_txn::StorageError>() {
            return storage_error_category(error);
        }
        current = error.source();
    }
    ExitCategory::Operational
}

fn storage_error_category(error: &strata_txn::StorageError) -> ExitCategory {
    match error {
        strata_txn::StorageError::LegacyFormatNeedsMigration(_)
        | strata_txn::StorageError::UnknownSchemaVersion { .. }
        | strata_txn::StorageError::MigrationSourceVersion { .. }
        | strata_txn::StorageError::MigrationUnsupportedDirection { .. }
        | strata_txn::StorageError::MigrationUnsupported { .. }
        | strata_txn::StorageError::MigrationIncompatibleType { .. }
        | strata_txn::StorageError::MigrationLossyConversion { .. }
        | strata_txn::StorageError::SchemaVersionChanged { .. }
        | strata_txn::StorageError::DurabilityUnsupported(_) => ExitCategory::Unsupported,
        strata_txn::StorageError::Serde(_)
        | strata_txn::StorageError::EmptyDataFile(_)
        | strata_txn::StorageError::CorruptManifest(_, _)
        | strata_txn::StorageError::MissingRowIdHighWater(_)
        | strata_txn::StorageError::CorruptDataFile(_, _) => ExitCategory::Corruption,
        strata_txn::StorageError::Io(_)
        | strata_txn::StorageError::Arrow(_)
        | strata_txn::StorageError::AlreadyExists(_)
        | strata_txn::StorageError::PublicationIndeterminate(_) => ExitCategory::Operational,
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
        "schema",
        "migration",
        "manifest-status",
        "recovery-status",
        "explain",
        "crash-loop",
        "lookup",
        "group-by",
        "query-scan",
    ];

    let Some(cmd) = args.get(1) else {
        eprintln!(
            "usage: strata <create|insert|scan|filter|search|explain|inspect|schema|migration|manifest-status|recovery-status|evidence|crash-loop|lookup|group-by|query-scan> <dir> [...]"
        );
        eprintln!(
            "  search <dir> --vector <comma-separated finite floats> [--k <usize>] [--filter <column> <op> <value>]"
        );
        eprintln!("  explain <dir> <column> <op> <value> [--json]");
        eprintln!("  lookup <dir> <row_id> [--columns <column,...>] [--json]");
        eprintln!(
            "  group-by <dir> <key,...> --agg <count|sum|avg:column> [--filter <column> <op> <value>]"
        );
        eprintln!("  query-scan <dir> --columns <column,...> [--filter <column> <op> <value>]");
        return Ok(());
    };
    if let Some(result) = handle_command_without_dataset(args, cmd) {
        return result;
    }
    if !KNOWN_COMMANDS.contains(&cmd.as_str()) {
        return Err(Box::new(CliError::UnknownCommand(cmd.clone())));
    }
    if cmd == "explain" {
        return handle_explain(args);
    }

    let dir = args
        .get(2)
        .ok_or_else(|| usage_error("missing <dir> argument"))?;
    run_dataset_command(args, cmd, dir)
}

fn handle_command_without_dataset(
    args: &[String],
    cmd: &str,
) -> Option<Result<(), Box<dyn Error>>> {
    if cmd == "help" {
        if args.len() != 2 {
            return Some(Err(usage_error(
                "help does not accept additional arguments",
            )));
        }
        println!(
            "usage: strata <create|insert|scan|filter|search|explain|inspect|schema|migration|manifest-status|recovery-status|evidence|crash-loop|lookup|group-by|query-scan> <dir> [...]"
        );
        println!("  explain <dir> <column> <op> <value> [--json]");
        println!("  lookup <dir> <row_id> [--columns <column,...>] [--json]");
        println!("  query-scan <dir> --columns <column,...> [--filter <column> <op> <value>]");
        return Some(Ok(()));
    }
    if cmd == "evidence" {
        return Some(handle_evidence(args));
    }
    if cmd == "migration" {
        return Some(handle_migration(args));
    }
    None
}

fn run_dataset_command(args: &[String], cmd: &str, dir: &str) -> Result<(), Box<dyn Error>> {
    match cmd {
        "create" => {
            handle_create(args, dir)?;
        }
        "insert" => {
            handle_insert(args, dir)?;
        }
        "scan" => {
            let ds = strata_txn::Dataset::open(dir)?;
            let snapshot = ds.snapshot();
            let (batch, header) = scan_summary(&snapshot, &strata_txn::mvp_fixtures::mvp_schema())?;
            println!("{header}");
            print_batch(&batch)?;
        }
        "filter" => {
            let name = args.get(3).ok_or("missing <name>")?;
            let ds = strata_txn::Dataset::open(dir)?;
            let batch = ds
                .snapshot()
                .scan(&strata_txn::mvp_fixtures::mvp_schema())?;
            let filtered = strata_query::filter_eq(&batch, "name", name)?;
            println!("{} matching rows", filtered.num_rows());
            print_batch(&filtered)?;
        }
        "search" => handle_search(args, dir)?,
        "inspect" => {
            let ds = strata_txn::Dataset::open(dir)?;
            let snapshot = ds.snapshot();
            let json = parse_json_flag(args, 3, "inspect")?;
            if json {
                let batch = snapshot.scan(&ds.schema())?;
                println!(
                    "{{\"kind\":\"inspect\",\"manifest_version\":{},\"schema_version\":{},\"row_count\":{}}}",
                    snapshot.version(),
                    ds.schema_version(),
                    batch.num_rows(),
                );
            } else {
                println!(
                    "{}",
                    inspect_summary(&snapshot, &strata_txn::mvp_fixtures::mvp_schema())?
                );
            }
        }
        "schema" => handle_schema(args, dir)?,
        "manifest-status" => handle_manifest_status(args, dir)?,
        "recovery-status" => handle_recovery_status(args, dir)?,
        "crash-loop" => {
            require_single_writer_ack(args, "crash-loop")?;
            let n: usize = args.get(3).ok_or("missing <num_commits>")?.parse()?;
            let ds = strata_txn::Dataset::open(dir)?;
            for i in 0..n {
                let mut txn = ds.begin();
                #[allow(clippy::cast_precision_loss)]
                txn.insert(strata_txn::mvp_fixtures::mvp_row(
                    i64::try_from(i)?,
                    "loop",
                    [i as f32, 0.0, 0.0],
                )?)?;
                txn.commit()?;
                println!("committed {}", ds.current_version());
                std::io::stdout().flush()?;
            }
        }
        "lookup" => handle_lookup(args, dir)?,
        "group-by" => handle_group_by(args, dir)?,
        "query-scan" => handle_query_scan(args, dir)?,
        other => return Err(Box::new(CliError::UnknownCommand(other.to_owned()))),
    }

    Ok(())
}

fn scan_summary(
    snapshot: &strata_txn::Snapshot,
    schema: &arrow::datatypes::SchemaRef,
) -> Result<(RecordBatch, String), Box<dyn Error>> {
    let batch = snapshot.scan(schema)?;
    let header = format_scan_header(batch.num_rows(), snapshot.version());
    Ok((batch, header))
}

fn inspect_summary(
    snapshot: &strata_txn::Snapshot,
    schema: &arrow::datatypes::SchemaRef,
) -> Result<String, Box<dyn Error>> {
    let batch = snapshot.scan(schema)?;
    Ok(format_inspect_line(snapshot.version(), batch.num_rows()))
}

fn format_scan_header(row_count: usize, snapshot_version: u64) -> String {
    format!("{row_count} rows at version {snapshot_version}")
}

fn format_inspect_line(snapshot_version: u64, row_count: usize) -> String {
    format!("version={snapshot_version} row_count={row_count}")
}

fn handle_schema(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    let json = parse_json_flag(args, 3, "schema")?;
    let dataset = strata_txn::Dataset::open(dir)?;
    let schema = dataset.schema();
    if json {
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                format!(
                    "{{\"name\":{},\"type\":{},\"nullable\":{}}}",
                    json_string(field.name()),
                    json_string(&schema_type_name(field.data_type())),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{{\"kind\":\"schema\",\"manifest_version\":{},\"schema_version\":{},\"fields\":[{fields}]}}",
            dataset.current_version(),
            dataset.schema_version(),
        );
    } else {
        println!(
            "schema manifest_version={} schema_version={}",
            dataset.current_version(),
            dataset.schema_version(),
        );
        for field in schema.fields() {
            println!(
                "field name={} type={} nullable={}",
                field.name(),
                schema_type_name(field.data_type()),
                field.is_nullable(),
            );
        }
    }
    Ok(())
}

fn handle_evidence(args: &[String]) -> Result<(), Box<dyn Error>> {
    let json = parse_json_flag(args, 2, "evidence")?;
    if json {
        println!(
            "{{\"kind\":\"evidence\",\"criterion_command\":\"cargo bench -p strata-bench --bench query_planner_bench\",\"report\":\"docs/phase-3-verification-report.md#task-3-query-planning-evidence\"}}"
        );
    } else {
        println!("criterion_command=cargo bench -p strata-bench --bench query_planner_bench");
        println!("report=docs/phase-3-verification-report.md#task-3-query-planning-evidence");
    }
    Ok(())
}

fn handle_migration(args: &[String]) -> Result<(), Box<dyn Error>> {
    let action = migration_action(args)?;
    let dir = args
        .get(3)
        .ok_or_else(|| usage_error("migration requires <validate|run|status> <dir> [...]"))?;

    if action == "status" {
        return handle_migration_status(args, dir);
    }

    let (column, data_type, json) = migration_request(args, action)?;
    if action == "run" {
        require_single_writer_ack(args, "migration run")?;
    }
    let dataset = strata_txn::Dataset::open(dir)?;
    let migration = strata_txn::SchemaMigration::add_nullable_column(
        dataset.schema_version(),
        strata_storage::ADD_NULLABLE_COLUMN_SCHEMA_VERSION,
        arrow::datatypes::Field::new(column, data_type.clone(), true),
    );

    validate_migration_target_schema(&migration, &dataset)?;
    if action == "validate" {
        print_migration_validation(&migration, column, &data_type, json);
        return Ok(());
    }

    let result = dataset.migrate_schema(&migration)?;
    print_migration_result(&result, json);
    Ok(())
}

fn migration_action(args: &[String]) -> Result<&str, Box<dyn Error>> {
    let action = args
        .get(2)
        .ok_or_else(|| usage_error("migration requires <validate|run|status> <dir> [...]"))?;
    if matches!(action.as_str(), "validate" | "run" | "status") {
        Ok(action)
    } else {
        Err(usage_error(
            "migration requires validate, run, or status as its first argument",
        ))
    }
}

fn handle_migration_status(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    let json = parse_json_flag(args, 4, "migration status")?;
    let dataset = strata_txn::Dataset::open(dir)?;
    if json {
        println!(
            "{{\"kind\":\"migration_status\",\"manifest_version\":{},\"schema_version\":{}}}",
            dataset.current_version(),
            dataset.schema_version(),
        );
    } else {
        println!(
            "migration manifest_version={} schema_version={}",
            dataset.current_version(),
            dataset.schema_version(),
        );
    }
    Ok(())
}

fn migration_request<'args>(
    args: &'args [String],
    action: &str,
) -> Result<(&'args str, DataType, bool), Box<dyn Error>> {
    let name = args.get(4).ok_or_else(|| {
        usage_error("migration validate/run requires add-nullable-column <column> <type>")
    })?;
    if name != "add-nullable-column" {
        return Err(usage_error(
            "migration validate/run requires the explicit add-nullable-column migration name",
        ));
    }
    let column = args.get(5).ok_or_else(|| {
        usage_error("migration validate/run requires add-nullable-column <column> <type>")
    })?;
    let data_type = migration_column_type(args.get(6).ok_or_else(|| {
        usage_error("migration validate/run requires add-nullable-column <column> <type>")
    })?)?;
    let json = if action == "run" {
        let non_acknowledgement_options = args
            .get(7..)
            .unwrap_or_default()
            .iter()
            .filter(|argument| argument.as_str() != ACK_SINGLE_WRITER)
            .cloned()
            .collect::<Vec<_>>();
        parse_json_flag(&non_acknowledgement_options, 0, "migration validate/run")?
    } else {
        parse_json_flag(args, 7, "migration validate/run")?
    };
    Ok((column, data_type, json))
}

fn print_migration_validation(
    migration: &strata_txn::SchemaMigration,
    column: &str,
    data_type: &DataType,
    json: bool,
) {
    if json {
        println!(
            "{{\"kind\":\"migration_validation\",\"name\":\"{}\",\"source_schema_version\":{},\"target_schema_version\":{},\"column\":{{\"name\":{},\"type\":{},\"nullable\":true}}}}",
            migration.name(),
            migration.source_version(),
            migration.target_version(),
            json_string(column),
            json_string(&schema_type_name(data_type)),
        );
    } else {
        println!(
            "migration validation name={} source_schema_version={} target_schema_version={} column={} type={} nullable=true",
            migration.name(),
            migration.source_version(),
            migration.target_version(),
            column,
            schema_type_name(data_type),
        );
    }
}

fn print_migration_result(result: &strata_txn::SchemaMigrationResult, json: bool) {
    if json {
        println!(
            "{{\"kind\":\"migration_result\",\"name\":\"{}\",\"source_schema_version\":{},\"target_schema_version\":{},\"manifest_version\":{}}}",
            result.name,
            result.source_schema_version,
            result.target_schema_version,
            result.manifest_version,
        );
    } else {
        println!(
            "migration result name={} source_schema_version={} target_schema_version={} manifest_version={}",
            result.name,
            result.source_schema_version,
            result.target_schema_version,
            result.manifest_version,
        );
    }
}

fn validate_migration_target_schema(
    migration: &strata_txn::SchemaMigration,
    dataset: &strata_txn::Dataset,
) -> Result<(), Box<dyn Error>> {
    let target_schema = migration.target_schema(dataset.schema_version(), &dataset.schema())?;
    strata_txn::Dataset::validate_schema(&target_schema).map_err(|error| match error {
        strata_txn::TxnError::ReservedColumnName(_) => usage_error(error.to_string()),
        other => Box::new(other) as Box<dyn Error>,
    })
}

fn handle_manifest_status(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    let json = parse_json_flag(args, 3, "manifest-status")?;
    let dataset = strata_txn::Dataset::open(dir)?;
    let report = dataset.lifecycle_report()?;
    if json {
        println!(
            "{{\"kind\":\"manifest_status\",\"observed_version\":{},\"schema_version\":{},\"manifest_object_count\":{},\"current_manifest_present\":{},\"data_object_count\":{},\"reachable_data_file_count\":{},\"reachable_segment_count\":{},\"orphan_candidate_count\":{}}}",
            report.observed_version(),
            dataset.schema_version(),
            report.manifest_object_count(),
            report.current_manifest_bytes().is_some(),
            report.data_object_count(),
            report.reachable_data_file_count(),
            report.reachable_segment_count(),
            report.orphan_candidate_count(),
        );
    } else {
        println!(
            "manifest observed_version={} schema_version={} manifest_object_count={} current_manifest_present={} data_object_count={} reachable_data_file_count={} reachable_segment_count={} orphan_candidate_count={}",
            report.observed_version(),
            dataset.schema_version(),
            report.manifest_object_count(),
            report.current_manifest_bytes().is_some(),
            report.data_object_count(),
            report.reachable_data_file_count(),
            report.reachable_segment_count(),
            report.orphan_candidate_count(),
        );
    }
    Ok(())
}

fn handle_recovery_status(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    let json = parse_json_flag(args, 3, "recovery-status")?;
    let dataset = strata_txn::Dataset::open(dir)?;
    let report = dataset.lifecycle_report()?;
    if json {
        println!(
            "{{\"kind\":\"recovery_status\",\"manifest_version\":{},\"schema_version\":{},\"physical_row_count\":{},\"tombstone_count\":{}}}",
            dataset.current_version(),
            dataset.schema_version(),
            report.physical_row_count(),
            report.tombstone_count(),
        );
    } else {
        println!(
            "recovery manifest_version={} schema_version={} physical_row_count={} tombstone_count={}",
            dataset.current_version(),
            dataset.schema_version(),
            report.physical_row_count(),
            report.tombstone_count(),
        );
    }
    Ok(())
}

fn migration_column_type(value: &str) -> Result<DataType, Box<dyn Error>> {
    match value {
        "boolean" => Ok(DataType::Boolean),
        "int64" => Ok(DataType::Int64),
        "uint64" => Ok(DataType::UInt64),
        "float64" => Ok(DataType::Float64),
        "utf8" => Ok(DataType::Utf8),
        _ => Err(usage_error(
            "migration column type must be boolean|int64|uint64|float64|utf8",
        )),
    }
}

fn parse_json_flag(args: &[String], start: usize, command: &str) -> Result<bool, Box<dyn Error>> {
    match args.get(start..) {
        Some([]) => Ok(false),
        Some([flag]) if flag == "--json" => Ok(true),
        _ => Err(usage_error(format!(
            "{command} accepts only an optional --json flag"
        ))),
    }
}

fn schema_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::UInt64 => "uint64".to_owned(),
        DataType::Float32 => "float32".to_owned(),
        DataType::Float64 => "float64".to_owned(),
        DataType::Utf8 => "utf8".to_owned(),
        DataType::FixedSizeList(field, dimensions) if field.data_type() == &DataType::Float32 => {
            format!("vector({dimensions})")
        }
        other => format!("unsupported({other:?})"),
    }
}

fn json_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('\"');
    for character in value.chars() {
        match character {
            '\"' => {
                escaped.push('\\');
                escaped.push('\"');
            }
            '\\' => {
                escaped.push('\\');
                escaped.push('\\');
            }
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let value = u32::from(character);
                escaped.push_str("\\u");
                for shift in [12, 8, 4, 0] {
                    let digit = ((value >> shift) & 0x0f) as usize;
                    escaped.push(char::from(HEX[digit]));
                }
            }
            character => escaped.push(character),
        }
    }
    escaped.push('\"');
    escaped
}

fn usage_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(CliError::Usage(message.into()))
}

fn require_single_writer_ack(args: &[String], command: &str) -> Result<(), Box<dyn Error>> {
    if args
        .iter()
        .skip(3)
        .any(|argument| argument == ACK_SINGLE_WRITER)
    {
        return Ok(());
    }
    Err(usage_error(format!(
        "{command} requires {ACK_SINGLE_WRITER}; {SINGLE_WRITER_BOUNDARY}"
    )))
}

fn print_single_writer_boundary() {
    println!(
        "acknowledgement=single-writer scope=one-process/shared-Dataset-handle not-cross-process-coordination not-serialization"
    );
}

fn handle_create(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    require_single_writer_ack(args, "create")?;
    strata_txn::Dataset::create(dir, strata_txn::mvp_fixtures::mvp_schema())?;
    println!("created dataset at {dir}");
    print_single_writer_boundary();
    Ok(())
}

fn handle_insert(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    require_single_writer_ack(args, "insert")?;
    let payload = args
        .iter()
        .skip(3)
        .filter(|argument| argument.as_str() != ACK_SINGLE_WRITER)
        .collect::<Vec<_>>();
    let id: i64 = payload
        .first()
        .ok_or_else(|| usage_error("insert requires <id> <name> <v0> <v1> <v2>"))?
        .parse()
        .map_err(|_| usage_error("insert <id> must be an Int64"))?;
    let name = payload
        .get(1)
        .ok_or_else(|| usage_error("insert requires <id> <name> <v0> <v1> <v2>"))?;
    let v0: f32 = payload
        .get(2)
        .ok_or_else(|| usage_error("insert requires <id> <name> <v0> <v1> <v2>"))?
        .parse()
        .map_err(|_| usage_error("insert <v0> must be a Float32"))?;
    let v1: f32 = payload
        .get(3)
        .ok_or_else(|| usage_error("insert requires <id> <name> <v0> <v1> <v2>"))?
        .parse()
        .map_err(|_| usage_error("insert <v1> must be a Float32"))?;
    let v2: f32 = payload
        .get(4)
        .ok_or_else(|| usage_error("insert requires <id> <name> <v0> <v1> <v2>"))?
        .parse()
        .map_err(|_| usage_error("insert <v2> must be a Float32"))?;
    if payload.len() > 5 {
        return Err(usage_error(
            "insert accepts <id> <name> <v0> <v1> <v2> and --ack-single-writer",
        ));
    }
    let ds = strata_txn::Dataset::open(dir)?;
    let mut txn = ds.begin();
    txn.insert(strata_txn::mvp_fixtures::mvp_row(id, name, [v0, v1, v2])?)?;
    txn.commit()?;
    println!("committed version {}", ds.current_version());
    print_single_writer_boundary();
    Ok(())
}

fn query_error(error: &strata_txn::QueryError) -> Box<dyn Error> {
    let message = error.to_string();
    let kind = match error {
        strata_txn::QueryError::Validation(_) => "validation",
        strata_txn::QueryError::Execution(_) => "execution",
    };
    Box::new(CliError::Query { kind, message })
}

fn parse_columns(value: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let columns = value
        .split(',')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if columns.is_empty() || columns.iter().any(String::is_empty) {
        return Err(usage_error(
            "--columns requires a non-empty comma-separated column list",
        ));
    }
    Ok(columns)
}

fn parse_comparison_operator(
    value: &str,
) -> Result<strata_txn::ComparisonOperator, Box<dyn Error>> {
    use strata_txn::ComparisonOperator;

    match value {
        "eq" => Ok(ComparisonOperator::Equal),
        "neq" => Ok(ComparisonOperator::NotEqual),
        "lt" => Ok(ComparisonOperator::LessThan),
        "lteq" => Ok(ComparisonOperator::LessThanOrEqual),
        "gt" => Ok(ComparisonOperator::GreaterThan),
        "gteq" => Ok(ComparisonOperator::GreaterThanOrEqual),
        _ => Err(usage_error("--filter <op> must be eq|neq|lt|lteq|gt|gteq")),
    }
}

fn parse_filter_literal(
    column: &str,
    value: &str,
    schema: &Schema,
) -> Result<strata_txn::FilterLiteral, Box<dyn Error>> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| usage_error(format!("unknown dataset column '{column}' in --filter")))?;
    match field.data_type() {
        DataType::Int64 => value
            .parse()
            .map(strata_txn::FilterLiteral::Int64)
            .map_err(|_| usage_error(format!("--filter value for '{column}' must be Int64"))),
        DataType::UInt64 => value
            .parse()
            .map(strata_txn::FilterLiteral::UInt64)
            .map_err(|_| usage_error(format!("--filter value for '{column}' must be UInt64"))),
        DataType::Float64 => value
            .parse()
            .map(strata_txn::FilterLiteral::Float64)
            .map_err(|_| usage_error(format!("--filter value for '{column}' must be Float64"))),
        DataType::Boolean => value
            .parse()
            .map(strata_txn::FilterLiteral::Boolean)
            .map_err(|_| usage_error(format!("--filter value for '{column}' must be Boolean"))),
        DataType::Utf8 => Ok(strata_txn::FilterLiteral::Utf8(value.to_owned())),
        _ => Err(usage_error(format!(
            "--filter column '{column}' is not a scalar column in the current CLI fixture schema"
        ))),
    }
}

fn parse_filter(
    args: &[String],
    start: usize,
    schema: &Schema,
) -> Result<(strata_txn::FilterExpression, usize), Box<dyn Error>> {
    let column = args
        .get(start)
        .ok_or_else(|| usage_error("missing <column> after --filter"))?;
    let operator = args
        .get(start + 1)
        .ok_or_else(|| usage_error("missing <op> after --filter"))?;
    let value = args
        .get(start + 2)
        .ok_or_else(|| usage_error("missing <value> after --filter"))?;
    Ok((
        strata_txn::FilterExpression::Compare(strata_txn::Comparison {
            column: column.clone(),
            operator: parse_comparison_operator(operator)?,
            value: parse_filter_literal(column, value, schema)?,
        }),
        start + 3,
    ))
}

fn format_value(value: &strata_txn::ResultValue) -> String {
    match value {
        strata_txn::ResultValue::Null => "Null".to_owned(),
        strata_txn::ResultValue::Boolean(value) => format!("Boolean({value})"),
        strata_txn::ResultValue::Int64(value) => format!("Int64({value})"),
        strata_txn::ResultValue::UInt64(value) => format!("UInt64({value})"),
        strata_txn::ResultValue::Float64(value) => format!("Float64({value})"),
        strata_txn::ResultValue::Utf8(value) => format!("Utf8({value:?})"),
        strata_txn::ResultValue::Vector(value) => format!("Vector({value:?})"),
    }
}

fn lookup_value_json(value: &strata_txn::ResultValue) -> String {
    match value {
        strata_txn::ResultValue::Null => "{\"type\":\"null\",\"value\":null}".to_owned(),
        strata_txn::ResultValue::Boolean(value) => {
            format!("{{\"type\":\"boolean\",\"value\":{value}}}")
        }
        strata_txn::ResultValue::Int64(value) => {
            format!("{{\"type\":\"int64\",\"value\":{value}}}")
        }
        strata_txn::ResultValue::UInt64(value) => {
            format!("{{\"type\":\"uint64\",\"value\":{value}}}")
        }
        strata_txn::ResultValue::Float64(value) => {
            // JSON has no NaN or infinity literals; preserve the logical
            // type while representing every non-finite value as null.
            let value = if value.is_finite() {
                value.to_string()
            } else {
                "null".to_owned()
            };
            format!("{{\"type\":\"float64\",\"value\":{value}}}")
        }
        strata_txn::ResultValue::Utf8(value) => {
            format!("{{\"type\":\"utf8\",\"value\":{}}}", json_string(value))
        }
        strata_txn::ResultValue::Vector(value) => {
            let values = value
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",");
            format!("{{\"type\":\"vector\",\"value\":[{values}]}}")
        }
    }
}

fn format_logical_type(data_type: &strata_txn::LogicalType) -> String {
    match data_type {
        strata_txn::LogicalType::Boolean => "Boolean".to_owned(),
        strata_txn::LogicalType::Int64 => "Int64".to_owned(),
        strata_txn::LogicalType::UInt64 => "UInt64".to_owned(),
        strata_txn::LogicalType::Float64 => "Float64".to_owned(),
        strata_txn::LogicalType::Utf8 => "Utf8".to_owned(),
        strata_txn::LogicalType::Vector { dimensions } => format!("Vector({dimensions})"),
    }
}

fn projected_row_line(prefix: &str, row: &strata_txn::ProjectedRow) -> String {
    let fields = row
        .fields
        .iter()
        .map(|field| format!("{}={}", field.name, format_value(&field.value)))
        .collect::<Vec<_>>()
        .join(" ");
    format!("{prefix} {fields}")
}

fn handle_lookup(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    let row_id = args
        .get(3)
        .ok_or_else(|| usage_error("lookup requires <row_id>"))?
        .parse()?;
    let (projection, json) = match args.get(4..) {
        Some([]) => (strata_txn::Projection::All, false),
        Some([flag]) if flag == "--json" => (strata_txn::Projection::All, true),
        Some([columns_flag]) if columns_flag == "--columns" => {
            return Err(usage_error("missing <column,...> after --columns"));
        }
        Some([columns_flag, json_flag]) if columns_flag == "--columns" && json_flag == "--json" => {
            return Err(usage_error("missing <column,...> after --columns"));
        }
        Some([columns_flag, columns]) if columns_flag == "--columns" => (
            strata_txn::Projection::Columns(parse_columns(columns)?),
            false,
        ),
        Some([columns_flag, columns, flag]) if columns_flag == "--columns" && flag == "--json" => (
            strata_txn::Projection::Columns(parse_columns(columns)?),
            true,
        ),
        _ => {
            return Err(usage_error(
                "lookup accepts optional --columns <column,...> and --json arguments",
            ));
        }
    };

    let result = strata_txn::Dataset::open(dir)?
        .snapshot()
        .lookup_row(&strata_txn::RowLookupRequest {
            row_id: strata_txn::RowId(row_id),
            projection,
        })
        .map_err(|error| query_error(&error))?;
    let (outcome, fields) = match result.outcome {
        strata_txn::RowLookupOutcome::Live(row) => ("live", row.fields),
        strata_txn::RowLookupOutcome::Tombstoned => ("tombstoned", Vec::new()),
        strata_txn::RowLookupOutcome::NotFound => ("not_found", Vec::new()),
    };
    if json {
        let fields = fields
            .iter()
            .map(|field| {
                format!(
                    "{{\"name\":{},\"value\":{}}}",
                    json_string(&field.name),
                    lookup_value_json(&field.value),
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{{\"kind\":\"lookup\",\"row_id\":{row_id},\"outcome\":\"{outcome}\",\"fields\":[{fields}]}}"
        );
    } else {
        println!("lookup row_id={row_id} outcome={outcome}");
        for field in fields {
            println!(
                "field name={} value={}",
                field.name,
                format_value(&field.value)
            );
        }
    }
    Ok(())
}

fn handle_query_scan(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    let dataset = strata_txn::Dataset::open(dir)?;
    let schema = dataset.schema();
    let mut columns = None;
    let mut filter = None;
    let mut index = 3;
    while index < args.len() {
        match args[index].as_str() {
            "--columns" if columns.is_none() => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| usage_error("missing <column,...> after --columns"))?;
                columns = Some(parse_columns(value)?);
                index += 2;
            }
            "--filter" if filter.is_none() => {
                let (parsed, next) = parse_filter(args, index + 1, schema.as_ref())?;
                filter = Some(parsed);
                index = next;
            }
            _ => {
                return Err(usage_error(
                    "query-scan requires --columns <column,...> and accepts one optional --filter <column> <op> <value>",
                ));
            }
        }
    }
    let projection =
        columns.ok_or_else(|| usage_error("query-scan requires --columns <column,...>"))?;
    let result = dataset
        .snapshot()
        .scan_query(&strata_txn::ScanRequest {
            projection: strata_txn::Projection::Columns(projection),
            filter,
        })
        .map_err(|error| query_error(&error))?;

    println!("query-scan projection={}", result.projection.join(","));
    let mut rows = result.rows;
    rows.sort_by_key(|row| projected_row_line("", row));
    // The typed scan contract excludes reserved physical `_row_id`; expose a
    // deterministic result index rather than presenting it as a physical ID.
    for (index, row) in rows.iter().enumerate() {
        println!("{}", projected_row_line(&format!("row index={index}"), row));
    }
    Ok(())
}

fn parse_aggregate(value: &str) -> Result<strata_txn::Aggregate, Box<dyn Error>> {
    let (function, column) = value
        .split_once(':')
        .ok_or_else(|| usage_error("--agg must be count|sum|avg:<column>"))?;
    let function = match function {
        "count" => strata_txn::AggregateFunction::Count,
        "sum" => strata_txn::AggregateFunction::Sum,
        "avg" => strata_txn::AggregateFunction::Average,
        _ => return Err(usage_error("--agg must be count|sum|avg:<column>")),
    };
    if column.is_empty() {
        return Err(usage_error("--agg must be count|sum|avg:<column>"));
    }
    Ok(strata_txn::Aggregate::new(
        column,
        function,
        value.replace(':', "_"),
    ))
}

fn handle_group_by(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    let dataset = strata_txn::Dataset::open(dir)?;
    let schema = dataset.schema();
    let keys = parse_columns(
        args.get(3)
            .ok_or_else(|| usage_error("group-by requires <key,...>"))?,
    )?;
    let mut aggregate = None;
    let mut filter = None;
    let mut index = 4;
    while index < args.len() {
        match args[index].as_str() {
            "--agg" if aggregate.is_none() => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| usage_error("missing <count|sum|avg:column> after --agg"))?;
                aggregate = Some(parse_aggregate(value)?);
                index += 2;
            }
            "--filter" if filter.is_none() => {
                let (parsed, next) = parse_filter(args, index + 1, schema.as_ref())?;
                filter = Some(parsed);
                index = next;
            }
            _ => {
                return Err(usage_error(
                    "group-by requires --agg <count|sum|avg:column> and accepts one optional --filter <column> <op> <value>",
                ));
            }
        }
    }
    let aggregate =
        aggregate.ok_or_else(|| usage_error("group-by requires --agg <count|sum|avg:column>"))?;
    let result = dataset
        .snapshot()
        .group_by_query(&strata_txn::GroupByRequest {
            group_by: keys,
            aggregates: vec![aggregate],
            filter,
        })
        .map_err(|error| query_error(&error))?;

    let aggregate_types = result
        .aggregates()
        .iter()
        .map(|aggregate| {
            format!(
                "{}:{}",
                aggregate.alias(),
                format_logical_type(aggregate.data_type())
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "group-by keys={} aggregates={aggregate_types}",
        result.group_by().join(",")
    );
    let mut rows = result.rows().to_vec();
    rows.sort_by_key(|row| {
        row.keys
            .iter()
            .map(format_value)
            .collect::<Vec<_>>()
            .join("\u{1f}")
    });
    for row in rows {
        let key_fields = result
            .group_by()
            .iter()
            .zip(&row.keys)
            .map(|(name, value)| format!("{name}={}", format_value(value)));
        let aggregate_fields = result
            .aggregates()
            .iter()
            .zip(&row.aggregates)
            .map(|(aggregate, value)| format!("{}={}", aggregate.alias(), format_value(value)));
        println!(
            "group {}",
            key_fields
                .chain(aggregate_fields)
                .collect::<Vec<_>>()
                .join(" ")
        );
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

fn handle_search(args: &[String], dir: &str) -> Result<(), Box<dyn Error>> {
    let exact = args.iter().any(|a| a == "--exact");
    if exact {
        return Err(usage_error(
            "search --exact is not supported; use the typed vector-search contract",
        ));
    }
    let dataset = strata_txn::Dataset::open(dir)?;
    let vector_idx = args.iter().position(|argument| argument == "--vector");
    let k_idx = args.iter().position(|argument| argument == "--k");
    let filter_idx = args.iter().position(|argument| argument == "--filter");
    let mut consumed = vec![false; args.len()];
    consumed[0..3.min(args.len())].fill(true);

    let vector = if let Some(idx) = vector_idx {
        if idx + 1 >= args.len() || args[idx + 1].starts_with("--") {
            return Err(usage_error(
                "--vector requires comma-separated finite floats",
            ));
        }
        consumed[idx] = true;
        consumed[idx + 1] = true;
        Some(parse_search_vector(&args[idx + 1])?)
    } else {
        None
    };

    let flagged_k = if let Some(idx) = k_idx {
        if idx + 1 >= args.len() || args[idx + 1].starts_with("--") {
            return Err(usage_error("--k requires a usize"));
        }
        consumed[idx] = true;
        consumed[idx + 1] = true;
        Some(
            args[idx + 1]
                .parse()
                .map_err(|_| usage_error("--k must be a usize"))?,
        )
    } else {
        None
    };

    let predicate = if let Some(idx) = filter_idx {
        consumed[idx] = true;
        let (filter, next) = parse_filter(args, idx + 1, dataset.schema().as_ref())?;
        for item in &mut consumed[idx + 1..next] {
            *item = true;
        }
        Some(filter)
    } else {
        None
    };

    let positional = args
        .iter()
        .enumerate()
        .skip(3)
        .filter_map(|(idx, argument)| (!consumed[idx]).then_some(argument))
        .collect::<Vec<_>>();
    if positional.iter().any(|argument| argument.starts_with("--")) {
        return Err(usage_error(
            "search accepts --vector, --k, and one optional --filter <column> <op> <value>",
        ));
    }

    let (query, k) = if let Some(query) = vector {
        if !positional.is_empty() {
            return Err(usage_error(
                "--vector cannot be combined with positional vector components",
            ));
        }
        (query, flagged_k.unwrap_or(3))
    } else {
        let v0: f32 = positional.first().ok_or("missing <v0>")?.parse()?;
        let v1: f32 = positional.get(1).ok_or("missing <v1>")?.parse()?;
        let v2: f32 = positional.get(2).ok_or("missing <v2>")?.parse()?;
        let k = if let Some(value) = positional.get(3) {
            value
                .parse()
                .map_err(|_| usage_error("legacy search k must be a usize"))?
        } else {
            3
        };
        (vec![v0, v1, v2], k)
    };

    // The result carries physical row ids, so the generic CLI does not depend
    // on an optional logical `id` column or a second scan.
    let result = dataset
        .snapshot()
        .vector_search_query(&strata_txn::VectorSearchRequest {
            vector_column: "vector".to_owned(),
            query,
            k,
            filter: predicate,
            hydration: strata_txn::VectorHydration::NotRequested,
        })
        .map_err(|error| query_error(&error))?;

    // The typed result already carries physical RowIds. Keep search generic
    // across datasets that do not define an `id` or `name` column and report
    // the squared-L2 distance alongside each physical RowId.
    for (row_id, squared_distance) in resolve_query_rows(&result)? {
        println!("row_id={row_id} squared_distance={squared_distance}");
    }
    Ok(())
}

fn parse_search_vector(value: &str) -> Result<Vec<f32>, Box<dyn Error>> {
    let vector = value
        .split(',')
        .map(str::trim)
        .map(|component| {
            component
                .parse::<f32>()
                .ok()
                .filter(|value| value.is_finite())
                .ok_or_else(|| usage_error("--vector must contain finite comma-separated floats"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if vector.is_empty() {
        return Err(usage_error(
            "--vector must contain finite comma-separated floats",
        ));
    }
    Ok(vector)
}

fn resolve_query_rows(
    result: &strata_txn::VectorSearchResult,
) -> Result<Vec<(u64, f32)>, Box<dyn Error>> {
    result
        .hits()
        .iter()
        .map(|hit| match &hit.hydration {
            strata_txn::VectorHydrationState::Unresolved(error) => {
                Err(Box::new(CliError::VectorResolution(format!(
                    "vector search returned unresolved row_id={} ({error})",
                    hit.row_id.0
                ))) as Box<dyn Error>)
            }
            strata_txn::VectorHydrationState::Hydrated(_)
            | strata_txn::VectorHydrationState::NotRequested => {
                Ok((hit.row_id.0, hit.squared_l2_distance))
            }
        })
        .collect()
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
        _ => Err(usage_error("explain <op> must be eq|lt|lteq|gt|gteq")),
    }
}

enum ExplainRequest<'args> {
    Legacy(strata_query::Predicate),
    Json {
        column: &'args str,
        operator: &'args str,
        value: &'args str,
    },
}

struct ExplainCommand<'args> {
    dir: &'args str,
    request: ExplainRequest<'args>,
}

fn parse_explain_command(args: &[String]) -> Result<ExplainCommand<'_>, Box<dyn Error>> {
    let dir = args
        .get(2)
        .filter(|argument| !argument.starts_with("--"))
        .map(String::as_str)
        .ok_or_else(|| usage_error("explain requires <dir> <column> <op> <value> [--json]"))?;
    let (column, operator, value, json) = match args.get(3..) {
        Some([column, operator, value]) => {
            (column.as_str(), operator.as_str(), value.as_str(), false)
        }
        Some([column, operator, value, flag]) if flag == "--json" => {
            (column.as_str(), operator.as_str(), value.as_str(), true)
        }
        _ => {
            return Err(usage_error(
                "explain requires <column> <op> <value> and accepts only an optional --json flag",
            ));
        }
    };
    if column.starts_with("--") || operator.starts_with("--") || value == "--json" {
        return Err(usage_error(
            "explain requires <column> <op> <value> and accepts only an optional --json flag",
        ));
    }

    let request = if json {
        parse_comparison_operator(operator)?;
        ExplainRequest::Json {
            column,
            operator,
            value,
        }
    } else {
        let value = value
            .parse()
            .map_err(|_| usage_error("explain <value> must be an Int64"))?;
        ExplainRequest::Legacy(parse_predicate(column, operator, value)?)
    };
    Ok(ExplainCommand { dir, request })
}

fn handle_explain(args: &[String]) -> Result<(), Box<dyn Error>> {
    let command = parse_explain_command(args)?;
    let ds = strata_txn::Dataset::open(command.dir)?;
    let predicate = match command.request {
        ExplainRequest::Json {
            column,
            operator,
            value,
        } => {
            let filter = strata_txn::FilterExpression::Compare(strata_txn::Comparison {
                column: column.to_owned(),
                operator: parse_comparison_operator(operator)?,
                value: parse_filter_literal(column, value, ds.schema().as_ref())?,
            });
            let plan = ds
                .snapshot()
                .explain_scan_query(&strata_txn::ScanRequest {
                    projection: strata_txn::Projection::All,
                    filter: Some(filter),
                })
                .map_err(|error| query_error(&error))?;
            println!("{}", explain_plan_json(&plan));
            return Ok(());
        }
        ExplainRequest::Legacy(predicate) => predicate,
    };
    let result = ds.snapshot().explain(&predicate);
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

fn explain_plan_json(plan: &strata_txn::PhysicalPlan) -> String {
    let logical = plan
        .logical_operators
        .iter()
        .map(logical_operator_name)
        .map(json_string)
        .collect::<Vec<_>>()
        .join(",");
    let physical = plan
        .physical_operators
        .iter()
        .map(physical_operator_name)
        .map(json_string)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"kind\":\"explain\",\"logical_operators\":[{logical}],\"physical_operators\":[{physical}],\"observations\":{{\"data_files_total\":{},\"data_files_scanned\":{},\"data_files_pruned\":{},\"index_segments_total\":{},\"index_segments_scanned\":{},\"index_segments_pruned\":{},\"transaction_overlay\":{}}}}}",
        plan.observations.data_files_total,
        plan.observations.data_files_scanned,
        plan.observations.data_files_pruned,
        plan.observations.index_segments_total,
        plan.observations.index_segments_scanned,
        plan.observations.index_segments_pruned,
        plan.observations.transaction_overlay,
    )
}

fn logical_operator_name(operator: &strata_txn::LogicalOperator) -> &'static str {
    match operator {
        strata_txn::LogicalOperator::Source => "source",
        strata_txn::LogicalOperator::Predicate { .. } => "predicate",
        strata_txn::LogicalOperator::Projection { .. } => "projection",
        strata_txn::LogicalOperator::Grouping { .. } => "grouping",
        strata_txn::LogicalOperator::VectorSearch { .. } => "vector_search",
        strata_txn::LogicalOperator::Materialize => "materialize",
    }
}

fn physical_operator_name(operator: &strata_txn::PhysicalOperator) -> &'static str {
    match operator {
        strata_txn::PhysicalOperator::ManifestSnapshotSource => "manifest_snapshot_source",
        strata_txn::PhysicalOperator::ZoneMapPruning => "zone_map_pruning",
        strata_txn::PhysicalOperator::TombstoneFilter => "tombstone_filter",
        strata_txn::PhysicalOperator::RowFilter => "row_filter",
        strata_txn::PhysicalOperator::ColumnProjection => "column_projection",
        strata_txn::PhysicalOperator::HashGroupBy => "hash_group_by",
        strata_txn::PhysicalOperator::FilterLiveSet => "filter_live_set",
        strata_txn::PhysicalOperator::ImmutableSegmentVectorSearch => {
            "immutable_segment_vector_search"
        }
        strata_txn::PhysicalOperator::HydrationLookup => "hydration_lookup",
        strata_txn::PhysicalOperator::TransactionOverlay => "transaction_overlay",
        strata_txn::PhysicalOperator::Materialize => "materialize",
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn lookup_json_uses_null_for_non_finite_float64_values() {
        // Stable contract: JSON has no non-finite numbers, so float64 NaN and
        // infinities serialize as a null value while retaining type=float64.
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let encoded = lookup_value_json(&strata_txn::ResultValue::Float64(value));
            let parsed: serde_json::Value = serde_json::from_str(&encoded).unwrap();

            assert_eq!(parsed["type"], "float64");
            assert!(parsed["value"].is_null());
        }
    }

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
    fn resolve_query_rows_uses_physical_ids_and_distances() {
        let result = strata_txn::VectorSearchResult::new(
            2,
            Some(vec!["id".to_owned()]),
            vec![
                strata_txn::VectorHit {
                    row_id: strata_txn::RowId(12),
                    squared_l2_distance: 1.5,
                    hydration: strata_txn::VectorHydrationState::Hydrated(
                        strata_txn::ProjectedRow {
                            fields: vec![strata_txn::ProjectedField::new(
                                "id",
                                strata_txn::ResultValue::Int64(300),
                            )],
                        },
                    ),
                },
                strata_txn::VectorHit {
                    row_id: strata_txn::RowId(10),
                    squared_l2_distance: 2.5,
                    hydration: strata_txn::VectorHydrationState::Hydrated(
                        strata_txn::ProjectedRow {
                            fields: vec![strata_txn::ProjectedField::new(
                                "id",
                                strata_txn::ResultValue::Int64(100),
                            )],
                        },
                    ),
                },
            ],
        )
        .unwrap();

        let resolved = resolve_query_rows(&result).unwrap();
        assert_eq!(resolved, vec![(12, 1.5), (10, 2.5)]);
    }

    #[test]
    fn resolve_query_rows_rejects_unresolved_matches() {
        let result = strata_txn::VectorSearchResult::new(
            1,
            Some(vec!["id".to_owned()]),
            vec![strata_txn::VectorHit {
                row_id: strata_txn::RowId(999),
                squared_l2_distance: 1.5,
                hydration: strata_txn::VectorHydrationState::Unresolved(
                    strata_txn::HydrationError::NotFound,
                ),
            }],
        )
        .unwrap();

        let error = resolve_query_rows(&result).unwrap_err();
        assert_eq!(
            error.to_string(),
            "vector search error: vector search returned unresolved row_id=999 (the matching row was not found)"
        );
    }

    #[test]
    fn query_error_formats_execution_kind() {
        let error =
            strata_txn::QueryError::from(strata_txn::QueryExecutionError::Int64SumOverflow {
                alias: "total".to_owned(),
            });
        assert_eq!(
            query_error(&error).to_string(),
            "query error kind=execution message=checked Int64 sum overflowed for aggregate 'total'"
        );
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
        let dir = tempfile::Builder::new()
            .prefix("strata-cli-explain-test-")
            .tempdir()
            .unwrap()
            .keep();
        let dir_str = dir.to_str().unwrap().to_string();
        strata_txn::Dataset::create(&dir_str, strata_txn::mvp_fixtures::mvp_schema()).unwrap();
        let ds = strata_txn::Dataset::open(&dir_str).unwrap();
        let mut txn = ds.begin();
        txn.insert(strata_txn::mvp_fixtures::mvp_row(1, "alice", [1.0, 2.0, 3.0]).unwrap())
            .unwrap();
        txn.commit().unwrap();

        let args = vec![
            "strata".to_string(),
            "explain".to_string(),
            dir_str.clone(),
            "id".to_string(),
            "eq".to_string(),
            "1".to_string(),
        ];
        let result = handle_explain(&args);
        assert!(result.is_ok(), "handle_explain failed: {result:?}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn conflict_errors_use_the_conflict_exit_category() {
        let error = strata_txn::TxnError::Conflict {
            contested_row_ids: vec![7],
        };

        assert_eq!(error_category(&error).code(), 3);
        assert_eq!(error_category(&error).name(), "conflict");
    }

    #[test]
    fn indeterminate_manifest_publication_uses_the_operational_exit_category() {
        let error =
            strata_txn::TxnError::Storage(strata_txn::StorageError::PublicationIndeterminate(
                "_versions/00000000000000000007.manifest".to_owned(),
            ));

        assert_eq!(error_category(&error).code(), 1);
        assert_eq!(error_category(&error).name(), "operational");
    }

    #[test]
    fn snapshot_label_uses_captured_snapshot_version_and_rows() {
        let dir = tempfile::Builder::new()
            .prefix("strata-cli-snapshot-label-test-")
            .tempdir()
            .unwrap()
            .keep();
        let dir_str = dir.to_str().unwrap().to_string();
        let schema = strata_txn::mvp_fixtures::mvp_schema();
        strata_txn::Dataset::create(&dir_str, schema.clone()).unwrap();
        let ds = strata_txn::Dataset::open(&dir_str).unwrap();

        let mut txn = ds.begin();
        txn.insert(strata_txn::mvp_fixtures::mvp_row(1, "alice", [1.0, 2.0, 3.0]).unwrap())
            .unwrap();
        txn.commit().unwrap();

        let snapshot = ds.snapshot();

        let mut txn = ds.begin();
        txn.insert(strata_txn::mvp_fixtures::mvp_row(2, "bob", [4.0, 5.0, 6.0]).unwrap())
            .unwrap();
        txn.commit().unwrap();

        let (batch, header) = scan_summary(&snapshot, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(header, "1 rows at version 1");
        assert_eq!(
            inspect_summary(&snapshot, &schema).unwrap(),
            "version=1 row_count=1"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn insert_scan_and_filter_subcommands_round_trip_through_the_cli() {
        let dir = tempfile::Builder::new()
            .prefix("strata-cli-subcommands-")
            .tempdir()
            .unwrap()
            .keep();
        let dir_str = dir.to_str().unwrap().to_string();

        run(&[
            "strata".to_string(),
            "create".to_string(),
            dir_str.clone(),
            "--ack-single-writer".to_string(),
        ])
        .unwrap();
        run(&[
            "strata".to_string(),
            "insert".to_string(),
            dir_str.clone(),
            "1".to_string(),
            "alice".to_string(),
            "1.0".to_string(),
            "2.0".to_string(),
            "3.0".to_string(),
            "--ack-single-writer".to_string(),
        ])
        .unwrap();
        run(&[
            "strata".to_string(),
            "insert".to_string(),
            dir_str.clone(),
            "2".to_string(),
            "bob".to_string(),
            "4.0".to_string(),
            "5.0".to_string(),
            "6.0".to_string(),
            "--ack-single-writer".to_string(),
        ])
        .unwrap();

        let ds = strata_txn::Dataset::open(&dir_str).unwrap();
        let scanned = ds
            .snapshot()
            .scan(&strata_txn::mvp_fixtures::mvp_schema())
            .unwrap();
        assert_eq!(
            scanned.num_rows(),
            2,
            "both inserted rows must be visible to scan"
        );

        // `scan`/`filter` themselves just print - confirm they run without
        // erroring against the dataset the two inserts above produced.
        assert!(run(&["strata".to_string(), "scan".to_string(), dir_str.clone()]).is_ok());
        assert!(
            run(&[
                "strata".to_string(),
                "filter".to_string(),
                dir_str.clone(),
                "alice".to_string(),
            ])
            .is_ok()
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_subcommand_prints_usage_and_returns_success() {
        // Pins down the currently-implicit behavior: no subcommand at all
        // is treated as a usage message, not an error (exit code SUCCESS).
        let result = run(&["strata".to_string()]);
        assert!(
            result.is_ok(),
            "a bare `strata` invocation with no subcommand must not error"
        );
    }
}
