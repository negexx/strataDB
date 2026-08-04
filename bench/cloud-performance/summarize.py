#!/usr/bin/env python3
"""Summarize deterministic before/after benchmark logs into JSONL and CSV."""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
from pathlib import Path
from typing import Any


MANIFEST_WALL = re.compile(
    r"median commit-sequence wall:\s*([0-9.]+) ms; p95:\s*([0-9.]+) ms"
    r"(?:; sample variance:\s*([0-9.]+) ms\^2)?"
)
MANIFEST_BYTES = re.compile(
    r"median newest manifest:\s*([0-9.]+) bytes; p95:\s*([0-9.]+) bytes"
    r"(?:; sample variance:\s*([0-9.]+) bytes\^2)?"
)
RSS = re.compile(r"Maximum resident set size \(kbytes\):\s*(\d+)")
SEGMENT_ROW = re.compile(
    r"^\s*(\d+)\s+([0-9.]+)\s+/\s+([0-9.]+)\s+"
    r"([0-9.]+)\s+/\s+([0-9.]+)\s+"
    r"([0-9.]+)\s+/\s+([0-9.]+)\s*$"
)
LIFECYCLE_MEASUREMENT = re.compile(r"measurement\s*:\s*(.+)")
LIFECYCLE_PINS = re.compile(r"pins=(\d+)")
LIFECYCLE_MANIFEST = re.compile(r"newest manifest bytes\s*:\s*(\d+)")
LIFECYCLE_PHASE = re.compile(
    r"^(?P<phase>[A-Za-z][A-Za-z0-9 +/(),_-]*?)\s+"
    r"(?P<wall>[0-9.]+)(?P<unit>ns|us|ms|s)\s+"
    r"(?P<allocated>[-+]?[0-9.]+)MB\s+"
    r"(?P<peak>[-+]?[0-9.]+)MB\s+"
    r"(?P<live>[-+]?[0-9.]+)MB\s+",
    re.MULTILINE,
)
LIFECYCLE_LOADING = re.compile(r"loading (\d+) rows \((\d+)-dim\) from ([^;]+); input hash=")
LIFECYCLE_INGEST_COMMITS = re.compile(r"(?m)^\s*(\d+) commits, [^\n]+$")
LIFECYCLE_DISTINCT = re.compile(r"(\d+) retained handles \((\d+) distinct snapshots;")
MANIFEST_INPUT = re.compile(
    r"commits=(\d+); buckets=\d+; warmup runs excluded=(\d+); measured repetitions=(\d+)"
)
SEGMENT_HEADER = re.compile(r"(\d+) rows x (\d+)-dim, k=(\d+), ef_search=(\d+)")
SEGMENT_PARAMS = re.compile(r"M=(\d+), ef_construction=(\d+), max_layer=(\d+)")
SEGMENT_QUERIES = re.compile(r"computing exact ground truth for (\d+) queries")
SEGMENT_LOADING = re.compile(r"loaded (\d+) rows from ([^;]+); input hash=")
SEGMENT_QUERY_POLICY = re.compile(
    r"query policy: (\d+) full unfiltered\+filtered warmup sweep\(s\), "
    r"then (\d+) measured sweep\(s\) per K"
)
EXPECTED_SEGMENTS = (1, 2, 4, 8, 16, 32, 64)
EXPECTED_PINS = (0, 1, 4, 16, 64)
FIXTURE_PROVENANCE = {
    "fixture_repo": "Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K",
    "fixture_revision": "56e6849a3d0f7913e56b475bf92c0064c93b576d",
    "fixture_file": "data/train-00000-of-00001.parquet",
    "fixture_size_bytes": "363758493",
    "fixture_sha256": "5ea400d91cba9b27fa55fc659e48f7bda8cba68443f087a15ddbc0e42acd049d",
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""


def _summary(values: list[float]) -> tuple[float, float]:
    ordered = sorted(values)
    p95_index = max(0, (len(ordered) * 95 + 99) // 100 - 1)
    return statistics.median(ordered), ordered[p95_index]


def _load_config(directory: Path, name: str = "config.env") -> dict[str, str]:
    config_path = directory / name
    if not config_path.is_file():
        return {}
    config: dict[str, str] = {}
    for line in config_path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            config[key] = value
    return config


def validate_fixture_provenance(config: dict[str, str]) -> None:
    """Reject fixture measurements that do not name the pinned input exactly."""
    if config.get("source") != "fixture":
        return
    for key, expected in FIXTURE_PROVENANCE.items():
        if config.get(key) != expected:
            raise ValueError(f"fixture provenance {key!r} must equal the pinned value")


def _base(label: str, benchmark: str, time_path: Path, config: dict[str, str]) -> dict[str, Any]:
    max_rss = RSS.search(_read(time_path))
    return {
        "label": label,
        "benchmark": benchmark,
        "max_rss_kb": int(max_rss.group(1)) if max_rss else None,
        "revision": config.get("revision"),
        "lockfile_sha256": config.get("lockfile_sha256"),
        "config": config,
    }


def _manifest(label: str, directory: Path, config: dict[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    paths = sorted(directory.glob("manifest_growth*.log"))
    for path in paths:
        benchmark = path.stem
        text = _read(path)
        wall = MANIFEST_WALL.search(text)
        manifest = MANIFEST_BYTES.search(text)
        if not wall or not manifest:
            continue
        base = _base(label, benchmark, directory / f"{benchmark}.time", config)
        rows.extend(
            [
                {
                    **base,
                    "metric": "wall_ms",
                    "value": float(wall.group(1)),
                    "p95": float(wall.group(2)),
                    "variance": float(wall.group(3)) if wall.group(3) else None,
                },
                {
                    **base,
                    "metric": "manifest_bytes",
                    "value": float(manifest.group(1)),
                    "p95": float(manifest.group(2)),
                    "variance": float(manifest.group(3)) if manifest.group(3) else None,
                },
            ]
        )
    return rows


def _segment(
    label: str, directory: Path, config: dict[str, str], benchmark: str = "segment_recall"
) -> list[dict[str, Any]]:
    text = _read(directory / f"{benchmark}.log")
    mode = None
    rows: list[dict[str, Any]] = []
    input_hashes = sorted(set(re.findall(r"input hash=([0-9a-f]+)", text)))
    base = _base(label, benchmark, directory / f"{benchmark}.time", config)
    base["input_hashes"] = input_hashes
    for line in text.splitlines():
        if line.startswith("unfiltered query results"):
            mode = "unfiltered"
        elif line.startswith("filtered query results"):
            mode = "filtered"
        match = SEGMENT_ROW.match(line)
        if not match or mode is None:
            continue
        segment, recall, recall_p95, latency, latency_p95, qps, qps_p95 = match.groups()
        prefix = f"segment_k{segment}_{mode}"
        values = {
            f"{prefix}_recall_median": float(recall),
            f"{prefix}_recall_p95": float(recall_p95),
            f"{prefix}_us_per_query_median": float(latency),
            f"{prefix}_us_per_query_p95": float(latency_p95),
            f"{prefix}_qps_median": float(qps),
            f"{prefix}_qps_p95": float(qps_p95),
        }
        rows.extend({**base, "metric": metric, "value": value, "p95": None} for metric, value in values.items())
    return rows


def _lifecycle(label: str, directory: Path, config: dict[str, str]) -> list[dict[str, Any]]:
    text = _read(directory / "lifecycle.log")
    base = _base(label, "lifecycle", directory / "lifecycle.time", config)
    input_hashes = sorted(set(re.findall(r"input hash=([0-9a-f]+)", text)))
    base["input_hashes"] = input_hashes
    samples: dict[str, list[float]] = {}
    blocks = re.split(r"(?=^================ StrataDB lifecycle)", text, flags=re.MULTILINE)
    for block in blocks:
        measurement = LIFECYCLE_MEASUREMENT.search(block)
        if not measurement or "measured repetition" not in measurement.group(1):
            continue
        pins_match = LIFECYCLE_PINS.search(measurement.group(1))
        if not pins_match:
            raise ValueError("measured lifecycle block is missing pins=... metadata")
        pins = pins_match.group(1)
        manifest = LIFECYCLE_MANIFEST.search(block)
        if manifest:
            samples.setdefault(f"lifecycle_pins{pins}_manifest_bytes", []).append(float(manifest.group(1)))
        for phase_match in LIFECYCLE_PHASE.finditer(block):
            phase = re.sub(r"[^a-z0-9]+", "_", phase_match.group("phase").lower()).strip("_")
            unit = phase_match.group("unit")
            factor = {"ns": 1e-6, "us": 1e-3, "ms": 1.0, "s": 1000.0}[unit]
            values = {
                "wall_ms": float(phase_match.group("wall")) * factor,
                "allocated_mb": float(phase_match.group("allocated")),
                "peak_live_mb": float(phase_match.group("peak")),
                "live_delta_mb": float(phase_match.group("live")),
            }
            for metric, value in values.items():
                samples.setdefault(f"lifecycle_pins{pins}_{phase}_{metric}", []).append(value)
    rows: list[dict[str, Any]] = []
    for metric, values in sorted(samples.items()):
        median, p95 = _summary(values)
        rows.append({**base, "metric": metric, "value": median, "p95": p95, "samples": len(values)})
    return rows


def summarize_directory(artifact: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for label in ("before", "after"):
        directory = artifact / label
        if not directory.is_dir():
            continue
        config = _load_config(directory)
        records.extend(_manifest(label, directory, config))
        records.extend(_segment(label, directory, config))
        records.extend(_lifecycle(label, directory, config))
        fixture_config = _load_config(directory, "fixture_segment_recall.env")
        if fixture_config:
            records.extend(_segment(label, directory, fixture_config, "fixture_segment_recall"))
    return records


def compute_deltas(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    indexed: dict[tuple[str, str, str], dict[str, Any]] = {
        (row["benchmark"], row["metric"], row["label"]): row
        for row in records
        if isinstance(row["value"], (int, float))
    }
    deltas: list[dict[str, Any]] = []
    keys = sorted({(benchmark, metric) for benchmark, metric, _ in indexed})
    for benchmark, metric in keys:
        before = indexed.get((benchmark, metric, "before"))
        after = indexed.get((benchmark, metric, "after"))
        if before is None or after is None:
            continue
        before_value = float(before["value"])
        after_value = float(after["value"])
        delta = after_value - before_value
        deltas.append(
            {
                "benchmark": benchmark,
                "metric": metric,
                "before": before_value,
                "after": after_value,
                "delta": delta,
                "relative_delta": delta / before_value if before_value else None,
            }
        )
    return deltas


def validate_records(records: list[dict[str, Any]], artifact: Path) -> None:
    by_label = {label: [row for row in records if row["label"] == label] for label in ("before", "after")}
    errors: list[str] = []
    configs = {label: _load_config(artifact / label) for label in by_label}
    if not configs["before"] or not configs["after"]:
        errors.append("both before/config.env and after/config.env are required")
    else:
        for label, config in configs.items():
            try:
                validate_fixture_provenance(config)
            except ValueError as error:
                errors.append(f"{label}: {error}")
        comparable_keys = sorted(set(configs["before"]) | set(configs["after"]))
        for key in comparable_keys:
            if key in {"label", "revision", "lockfile_sha256"}:
                continue
            if configs["before"].get(key) != configs["after"].get(key):
                errors.append(f"workload provenance mismatch for {key!r}")
    for label, rows in by_label.items():
        directory = artifact / label
        config = configs[label]
        for benchmark in tuple(f"manifest_growth_{point}" for point in (1, 10, 20, 40, 80, 160)) + (
            "segment_recall",
            "lifecycle",
        ):
            for suffix in (".log", ".time"):
                if not (directory / f"{benchmark}{suffix}").is_file():
                    errors.append(f"{label}: missing {benchmark}{suffix}")
            if not any(row["benchmark"] == benchmark for row in rows):
                errors.append(f"{label}: no parsed metrics for {benchmark}")
        metrics = {row["metric"] for row in rows}
        for point in (1, 10, 20, 40, 80, 160):
            for metric in ("wall_ms", "manifest_bytes"):
                benchmark = f"manifest_growth_{point}"
                point_rows = [
                    row for row in rows if row["benchmark"] == benchmark and row["metric"] == metric
                ]
                if not point_rows:
                    errors.append(f"{label}: missing {benchmark}/{metric}")
                elif any(row.get("variance") is None for row in point_rows):
                    errors.append(f"{label}: missing variance for {benchmark}/{metric}")
            manifest_text = _read(directory / f"manifest_growth_{point}.log")
            manifest_input = MANIFEST_INPUT.search(manifest_text)
            expected_manifest = (
                point,
                int(config.get("manifest_warmup_runs", "0")),
                int(config.get("manifest_repetitions", "0")),
            )
            if not manifest_input or tuple(map(int, manifest_input.groups())) != expected_manifest:
                errors.append(f"{label}: manifest_growth_{point} emitted protocol does not match config")
        segment_text = _read(directory / "segment_recall.log")
        segment_loading = SEGMENT_LOADING.search(segment_text)
        segment_header = SEGMENT_HEADER.search(segment_text)
        segment_params = SEGMENT_PARAMS.search(segment_text)
        segment_queries = SEGMENT_QUERIES.search(segment_text)
        segment_query_policy = SEGMENT_QUERY_POLICY.search(segment_text)
        expected_source = f"synthetic seed={config.get('seed', '')}"
        if not segment_loading or (
            segment_loading.group(1) != config.get("segment_rows", "0")
            or segment_loading.group(2) != expected_source
        ):
            errors.append(f"{label}: segment input metadata does not match config")
        expected_segment = (
            int(config.get("segment_rows", "0")),
            int(config.get("segment_dimension", "0")),
            int(config.get("segment_k", "0")),
            int(config.get("segment_ef_search", "0")),
        )
        if not segment_header or tuple(map(int, segment_header.groups())) != expected_segment:
            errors.append(f"{label}: segment header does not match configured rows/dimension/k/ef_search")
        expected_params = (
            int(config.get("segment_m", "0")),
            int(config.get("segment_ef_construction", "0")),
            int(config.get("segment_max_layer", "0")),
        )
        if not segment_params or tuple(map(int, segment_params.groups())) != expected_params:
            errors.append(f"{label}: segment HNSW parameters do not match the configured workload")
        if not segment_queries or int(segment_queries.group(1)) != int(config.get("segment_queries", "0")):
            errors.append(f"{label}: segment query count does not match config")
        expected_query_policy = (
            int(config.get("segment_warmup_runs", "0")),
            int(config.get("segment_repetitions", "0")),
        )
        if not segment_query_policy or tuple(map(int, segment_query_policy.groups())) != expected_query_policy:
            errors.append(f"{label}: segment warmup/repetition policy does not match config")
        lifecycle_text = _read(directory / "lifecycle.log")
        loadings = LIFECYCLE_LOADING.findall(lifecycle_text)
        expected_loading = (
            config.get("lifecycle_rows", "0"),
            "512",
            expected_source,
        )
        if not loadings or any(loading != expected_loading for loading in loadings):
            errors.append(f"{label}: lifecycle input metadata does not match config")
        commit_counts = [int(value) for value in LIFECYCLE_INGEST_COMMITS.findall(lifecycle_text)]
        lifecycle_rows_config = int(config.get("lifecycle_rows", "0"))
        batch_rows = int(config.get("lifecycle_batch_rows", "1"))
        expected_commits = (lifecycle_rows_config + batch_rows - 1) // batch_rows
        if not commit_counts or any(count != expected_commits for count in commit_counts):
            errors.append(f"{label}: lifecycle commit count does not match configured batch size")
        distinct_counts = [int(value) for _, value in LIFECYCLE_DISTINCT.findall(lifecycle_text)]
        expected_distinct = sorted(int(value) for value in config.get("lifecycle_pins", "").split(","))
        if sorted(set(distinct_counts)) != expected_distinct:
            errors.append(f"{label}: lifecycle distinct-snapshot counts do not match configured pins")
        for segment in EXPECTED_SEGMENTS:
            for mode in ("unfiltered", "filtered"):
                metric = f"segment_k{segment}_{mode}_us_per_query_median"
                if metric not in metrics:
                    errors.append(f"{label}: missing segment_recall/{metric}")
        for pins in EXPECTED_PINS:
            metric = f"lifecycle_pins{pins}_ingest_commit_wall_ms"
            lifecycle_rows = [
                row for row in rows if row["benchmark"] == "lifecycle" and row["metric"] == metric
            ]
            if not lifecycle_rows:
                errors.append(f"{label}: missing lifecycle/{metric}")
            else:
                expected_samples = int(configs[label].get("lifecycle_repetitions", "0"))
                if any(row.get("samples") != expected_samples for row in lifecycle_rows):
                    errors.append(
                        f"{label}: lifecycle/{metric} does not contain {expected_samples} measured repetitions"
                    )
        for benchmark in tuple(f"manifest_growth_{point}" for point in (1, 10, 20, 40, 80, 160)) + (
            "segment_recall",
            "lifecycle",
        ):
            if not any(row["benchmark"] == benchmark and row["max_rss_kb"] is not None for row in rows):
                errors.append(f"{label}: missing GNU time RSS metric for {benchmark}")
        for benchmark in ("segment_recall", "lifecycle"):
            hashes = {
                tuple(row.get("input_hashes", []))
                for row in rows
                if row["benchmark"] == benchmark
            }
            if len(hashes) != 1 or len(next(iter(hashes), ())) != 1:
                errors.append(f"{label}: {benchmark} must contain exactly one input hash")
    for benchmark in ("segment_recall", "lifecycle"):
        before_hashes = {
            tuple(row.get("input_hashes", []))
            for row in by_label["before"]
            if row["benchmark"] == benchmark
        }
        after_hashes = {
            tuple(row.get("input_hashes", []))
            for row in by_label["after"]
            if row["benchmark"] == benchmark
        }
        if before_hashes != after_hashes:
            errors.append(f"before/after input hashes differ for {benchmark}")
    fixture_configs = {
        label: _load_config(artifact / label, "fixture_segment_recall.env") for label in by_label
    }
    fixture_present = {label for label, config in fixture_configs.items() if config}
    if fixture_present and fixture_present != {"before", "after"}:
        errors.append("fixture_segment_recall provenance is required for both before and after")
    if fixture_present == {"before", "after"}:
        for label, config in fixture_configs.items():
            try:
                validate_fixture_provenance(config)
            except ValueError as error:
                errors.append(f"{label}: {error}")
            directory = artifact / label
            log_path = directory / "fixture_segment_recall.log"
            time_path = directory / "fixture_segment_recall.time"
            if not log_path.is_file() or not time_path.is_file():
                errors.append(f"{label}: missing fixture_segment_recall log or time output")
                continue
            fixture_log = _read(log_path)
            fixture_loading = SEGMENT_LOADING.search(fixture_log)
            if not fixture_loading or not fixture_loading.group(2).startswith("fixture "):
                errors.append(f"{label}: fixture_segment_recall did not emit a fixture input source")
            elif fixture_loading.group(1) != config.get("segment_rows", ""):
                errors.append(f"{label}: fixture_segment_recall row count does not match config")
            fixture_rows = [
                row
                for row in by_label[label]
                if row["benchmark"] == "fixture_segment_recall"
            ]
            if not fixture_rows:
                errors.append(f"{label}: no parsed fixture_segment_recall metrics")
        fixture_hashes = {
            label: {
                tuple(row.get("input_hashes", []))
                for row in by_label[label]
                if row["benchmark"] == "fixture_segment_recall"
            }
            for label in by_label
        }
        if any(len(hashes) != 1 or len(next(iter(hashes), ())) != 1 for hashes in fixture_hashes.values()):
            errors.append("fixture_segment_recall must contain exactly one input hash per revision")
        elif fixture_hashes["before"] != fixture_hashes["after"]:
            errors.append("before/after fixture_segment_recall input hashes differ")
    if errors:
        raise ValueError("incomplete before/after evidence:\n" + "\n".join(errors))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--jsonl", type=Path, default=None)
    parser.add_argument("--csv", dest="csv_path", type=Path, default=None)
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    records = summarize_directory(args.artifact)
    deltas = compute_deltas(records)
    if args.validate:
        try:
            validate_records(records, args.artifact)
        except ValueError as error:
            raise SystemExit(str(error)) from error
        if not deltas:
            raise SystemExit("no directly comparable before/after numeric metrics found")
    output = records + [{"kind": "delta", **row} for row in deltas]
    if args.jsonl:
        args.jsonl.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in output), encoding="utf-8")
    if args.csv_path:
        fields = sorted({field for row in deltas for field in row})
        with args.csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(deltas)
    print(json.dumps({"records": len(records), "deltas": len(deltas)}, sort_keys=True))


if __name__ == "__main__":
    main()
