//! `strata` CLI — dataset/manifest inspection, and the Phase 1 MVP checklist
//! surface. `crash-loop` exists specifically to be killed mid-write by
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
    Query { kind: &'static str, message: String },
    VectorResolution(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage(message) => write!(formatter, "usage error: {message}"),
            Self::Query { kind, message } => {
                write!(formatter, "query error kind={kind} message={message}")
            }
            Self::VectorResolution(message) => write!(formatter, "vector search error: {message}"),
        }
    }
}

impl Error for CliError {}

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
        "lookup",
        "group-by",
        "query-scan",
    ];

    let Some(cmd) = args.get(1) else {
        eprintln!(
            "usage: strata <create|insert|scan|filter|search|explain|inspect|crash-loop|lookup|group-by|query-scan> <dir> [...]"
        );
        eprintln!(
            "  search <dir> --vector <comma-separated finite floats> [--k <usize>] [--filter <column> <op> <value>]"
        );
        eprintln!("  lookup <dir> <row_id> [--columns <column,...>]");
        eprintln!(
            "  group-by <dir> <key,...> --agg <count|sum|avg:column> [--filter <column> <op> <value>]"
        );
        eprintln!("  query-scan <dir> --columns <column,...> [--filter <column> <op> <value>]");
        return Ok(());
    };
    if !KNOWN_COMMANDS.contains(&cmd.as_str()) {
        return Err(format!("unknown command: {cmd}").into());
    }

    let dir = args.get(2).ok_or("missing <dir> argument")?;

    match cmd.as_str() {
        "create" => {
            handle_create(args, dir)?;
        }
        "insert" => {
            handle_insert(args, dir)?;
        }
        "scan" => {
            let ds = strata_txn::Dataset::open(dir)?;
            let batch = ds
                .snapshot()
                .scan(&strata_txn::mvp_fixtures::mvp_schema())?;
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
            let batch = ds
                .snapshot()
                .scan(&strata_txn::mvp_fixtures::mvp_schema())?;
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
        other => return Err(format!("unknown command: {other}").into()),
    }

    Ok(())
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
    let projection = match args.get(4).map(String::as_str) {
        None => strata_txn::Projection::All,
        Some("--columns") => strata_txn::Projection::Columns(parse_columns(
            args.get(5)
                .ok_or_else(|| usage_error("missing <column,...> after --columns"))?,
        )?),
        Some(_) => {
            return Err(usage_error(
                "lookup accepts only an optional --columns <column,...> argument",
            ));
        }
    };
    if args.len() > 6 {
        return Err(usage_error(
            "lookup accepts only an optional --columns <column,...> argument",
        ));
    }

    let result = strata_txn::Dataset::open(dir)?
        .snapshot()
        .lookup_row(&strata_txn::RowLookupRequest {
            row_id: strata_txn::RowId(row_id),
            projection,
        })
        .map_err(|error| query_error(&error))?;
    match result.outcome {
        strata_txn::RowLookupOutcome::Live(row) => {
            println!("lookup row_id={row_id} outcome=live");
            for field in row.fields {
                println!(
                    "field name={} value={}",
                    field.name,
                    format_value(&field.value)
                );
            }
        }
        strata_txn::RowLookupOutcome::Tombstoned => {
            println!("lookup row_id={row_id} outcome=tombstoned");
        }
        strata_txn::RowLookupOutcome::NotFound => {
            println!("lookup row_id={row_id} outcome=not_found");
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
        other => Err(format!("unknown op: {other} (expected eq|lt|lteq|gt|gteq)").into()),
    }
}

fn handle_explain(dir: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
    let column = args.get(3).ok_or("missing <column>")?;
    let op = args.get(4).ok_or("missing <op: eq|lt|lteq|gt|gteq>")?;
    let value: i64 = args.get(5).ok_or("missing <value>")?.parse()?;
    let predicate = parse_predicate(column, op, value)?;

    let ds = strata_txn::Dataset::open(dir)?;
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
        let result = handle_explain(&dir_str, &args);
        assert!(result.is_ok(), "handle_explain failed: {result:?}");

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
