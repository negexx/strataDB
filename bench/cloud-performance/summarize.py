#!/usr/bin/env python3
"""Summarize deterministic before/after benchmark logs into JSONL and CSV."""

from __future__ import annotations

import argparse
from collections import Counter
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
LIFECYCLE_LOADING = re.compile(r"loading (\d+) rows \((\d+)-dim\) from ([^;]+); input hash=([0-9a-f]+)")
LIFECYCLE_INGEST_COMMITS = re.compile(r"(?m)^\s*(\d+) commits, [^\n]+$")
LIFECYCLE_DISTINCT = re.compile(r"(\d+) retained handles \((\d+) distinct snapshots;")
MANIFEST_INPUT = re.compile(
    r"commits=(\d+); buckets=\d+; warmup runs excluded=(\d+); measured repetitions=(\d+)"
)
SEGMENT_HEADER = re.compile(r"(\d+) rows x (\d+)-dim, k=(\d+), ef_search=(\d+)")
SEGMENT_PARAMS = re.compile(r"M=(\d+), ef_construction=(\d+), max_layer=(\d+)")
SEGMENT_QUERIES = re.compile(r"computing exact ground truth for (\d+) queries")
SEGMENT_LOADING = re.compile(r"loaded (\d+) rows from ([^;]+); input hash=([0-9a-f]+)")
SEGMENT_QUERY_POLICY = re.compile(
    r"query policy: (\d+) full unfiltered\+filtered warmup sweep\(s\), "
    r"then (\d+) measured sweep\(s\) per K"
)
EXPECTED_SEGMENTS = (1, 2, 4, 8, 16, 32, 64)
EXPECTED_SEGMENT_MODES = ("unfiltered", "filtered")
SEGMENT_METRIC_SUFFIXES = (
    "recall_median",
    "recall_p95",
    "us_per_query_median",
    "us_per_query_p95",
    "qps_median",
    "qps_p95",
)
EXPECTED_PINS = (0, 1, 4, 16, 64)
RAW_PROVENANCE_KEYS = ("label", "revision", "lockfile_sha256")
FIXTURE_PROVENANCE = {
    "fixture_repo": "Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K",
    "fixture_revision": "56e6849a3d0f7913e56b475bf92c0064c93b576d",
    "fixture_file": "data/train-00000-of-00001.parquet",
    "fixture_size_bytes": "363758493",
    "fixture_sha256": "5ea400d91cba9b27fa55fc659e48f7bda8cba68443f087a15ddbc0e42acd049d",
}
FIXTURE_LIFECYCLE_PROTOCOL = {
    "fixture_lifecycle_rows": "100000",
    "fixture_lifecycle_batch_rows": "5000",
    "fixture_lifecycle_pins": "1",
    "fixture_lifecycle_warmup_runs": "1",
    "fixture_lifecycle_repetitions": "5",
    "fixture_lifecycle_protocol": "fixture-100000-rows-batch-5000-pins-1-warmups-1-repetitions-5",
}
EXPECTED_FIXTURE_LIFECYCLE_MEASUREMENTS = (
    "excluded warmup 1/1; pins=1",
    "measured repetition 1/5; pins=1",
    "measured repetition 2/5; pins=1",
    "measured repetition 3/5; pins=1",
    "measured repetition 4/5; pins=1",
    "measured repetition 5/5; pins=1",
)
EXPECTED_LIFECYCLE_PHASES = {
    "ingest_commit",
    "recovery_reopen",
    "pinned_snapshot_cache_residency",
    "full_scan",
    "filtered_scan",
    "group_by_4_aggs",
    "vector_search_unfiltered",
    "vector_search_filtered",
    "vector_search_filtered_varying_predicate",
    "concurrent_commits",
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


def _log_values(text: str, key: str) -> list[str]:
    return re.findall(rf"(?m)^{re.escape(key)}=(.*)$", text)


def validate_fixture_provenance(config: dict[str, str]) -> None:
    """Reject fixture measurements that do not name the pinned input exactly."""
    if config.get("source") != "fixture":
        return
    for key, expected in FIXTURE_PROVENANCE.items():
        if config.get(key) != expected:
            raise ValueError(f"fixture provenance {key!r} must equal the pinned value")


def _exact_values(text: str, key: str) -> list[str]:
    return _log_values(text, key)


def _validate_raw_provenance(
    directory: Path, benchmark: str, config: dict[str, str], label: str, errors: list[str]
) -> None:
    text = _read(directory / f"{benchmark}.log")
    for key in RAW_PROVENANCE_KEYS:
        if _exact_values(text, key) != [config.get(key)]:
            errors.append(
                f"{label}: {benchmark}.log must emit {key} exactly once matching config.env"
            )


def _configured_points(
    config: dict[str, str], key: str, expected: tuple[int, ...], label: str, errors: list[str]
) -> tuple[int, ...]:
    try:
        points = tuple(int(value) for value in config.get(key, "").split(","))
    except ValueError:
        points = ()
    if points != expected:
        errors.append(f"{label}: {key} must equal {','.join(map(str, expected))}")
    return expected


def _validate_segment_log(
    text: str,
    config: dict[str, str],
    expected_source: str,
    label: str,
    benchmark: str,
    errors: list[str],
    expected_hash: str | None = None,
) -> None:
    loadings = SEGMENT_LOADING.findall(text)
    expected_rows = config.get("segment_rows", "0")
    if len(loadings) != 1 or loadings[0][:2] != (expected_rows, expected_source):
        errors.append(f"{label}: {benchmark} input metadata does not match config exactly once")
    hashes = re.findall(r"input hash=([0-9a-f]+)", text)
    expected_hashes = [expected_hash] if expected_hash is not None else [loadings[0][2]] if len(loadings) == 1 else []
    if hashes != expected_hashes:
        hash_name = "fixture_input_hash" if expected_hash is not None else "input hash"
        errors.append(f"{label}: {benchmark} must emit exactly one consistent {hash_name}")
    expected_header = (
        int(config.get("segment_rows", "0")),
        int(config.get("segment_dimension", "0")),
        int(config.get("segment_k", "0")),
        int(config.get("segment_ef_search", "0")),
    )
    if SEGMENT_HEADER.findall(text) != [tuple(map(str, expected_header))]:
        errors.append(f"{label}: {benchmark} vector shape does not match config exactly once")
    expected_params = (
        int(config.get("segment_m", "0")),
        int(config.get("segment_ef_construction", "0")),
        int(config.get("segment_max_layer", "0")),
    )
    if SEGMENT_PARAMS.findall(text) != [tuple(map(str, expected_params))]:
        errors.append(f"{label}: {benchmark} HNSW parameters do not match config exactly once")
    expected_queries = str(config.get("segment_queries", "0"))
    if SEGMENT_QUERIES.findall(text) != [expected_queries]:
        errors.append(f"{label}: {benchmark} query count does not match config exactly once")
    expected_policy = (
        str(config.get("segment_warmup_runs", "0")),
        str(config.get("segment_repetitions", "0")),
    )
    if SEGMENT_QUERY_POLICY.findall(text) != [expected_policy]:
        errors.append(f"{label}: {benchmark} warmup/repetition policy does not match config exactly once")


def _validate_segment_matrix(
    rows: list[dict[str, Any]], label: str, benchmark: str, points: tuple[int, ...], errors: list[str]
) -> None:
    expected = Counter(
        f"segment_k{point}_{mode}_{suffix}"
        for point in points
        for mode in EXPECTED_SEGMENT_MODES
        for suffix in SEGMENT_METRIC_SUFFIXES
    )
    actual = Counter(row["metric"] for row in rows if row["benchmark"] == benchmark)
    if actual != expected:
        missing = sorted((expected - actual).elements())
        duplicate_or_unexpected = sorted((actual - expected).elements())
        details = ", ".join(missing + duplicate_or_unexpected)
        errors.append(f"{label}: incomplete {benchmark} segment metric matrix: {details}")


def _validate_single_rss(directory: Path, benchmark: str, label: str, errors: list[str]) -> None:
    if len(RSS.findall(_read(directory / f"{benchmark}.time"))) != 1:
        errors.append(f"{label}: {benchmark}.time must contain exactly one GNU time RSS metric")


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


def _lifecycle(
    label: str, directory: Path, config: dict[str, str], benchmark: str = "lifecycle"
) -> list[dict[str, Any]]:
    text = _read(directory / f"{benchmark}.log")
    base = _base(label, benchmark, directory / f"{benchmark}.time", config)
    input_hashes = sorted(set(re.findall(r"input hash=([0-9a-f]+)", text)))
    base["input_hashes"] = input_hashes
    samples: dict[str, list[float]] = {}
    blocks = re.split(r"(?=^================ StrataDB lifecycle)", text, flags=re.MULTILINE)
    for block in blocks:
        if not re.search(r"^================ StrataDB lifecycle", block, flags=re.MULTILINE):
            continue
        markers = LIFECYCLE_MEASUREMENT.findall(block)
        if len(markers) != 1:
            raise ValueError("lifecycle block must contain exactly one measurement marker")
        measurement = markers[0].strip()
        phase_names = [_lifecycle_phase_name(match) for match in LIFECYCLE_PHASE.finditer(block)]
        if benchmark == "fixture_lifecycle" and Counter(phase_names) != Counter(EXPECTED_LIFECYCLE_PHASES):
            raise ValueError("lifecycle block phases do not match the exact emitted set")
        if "measured repetition" not in measurement:
            continue
        pins_match = LIFECYCLE_PINS.search(measurement)
        if not pins_match:
            raise ValueError("measured lifecycle block is missing pins=... metadata")
        pins = pins_match.group(1)
        manifest = LIFECYCLE_MANIFEST.search(block)
        if manifest:
            samples.setdefault(f"lifecycle_pins{pins}_manifest_bytes", []).append(float(manifest.group(1)))
        for phase_match in LIFECYCLE_PHASE.finditer(block):
            phase = _lifecycle_phase_name(phase_match)
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


def _lifecycle_phase_name(match: re.Match[str]) -> str:
    return re.sub(r"[^a-z0-9]+", "_", match.group("phase").lower()).strip("_")


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
        fixture_lifecycle_config = _load_config(directory, "fixture_lifecycle.env")
        if fixture_lifecycle_config:
            records.extend(_lifecycle(label, directory, fixture_lifecycle_config, "fixture_lifecycle"))
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
            config_text = _read(artifact / label / "config.env")
            for key, value in config.items():
                if _exact_values(config_text, key) != [value]:
                    errors.append(f"{label}: config.env must emit {key} exactly once")
            try:
                validate_fixture_provenance(config)
            except ValueError as error:
                errors.append(f"{label}: {error}")
            if config.get("source") != "synthetic":
                errors.append(f"{label}: config.env source must be synthetic")
            if config.get("fixture_evidence") not in {"requested", "not-requested"}:
                errors.append(f"{label}: fixture_evidence must be requested or not-requested")
            for key, expected in FIXTURE_LIFECYCLE_PROTOCOL.items():
                if config.get(key) != expected:
                    errors.append(f"{label}: {key} must equal {expected}")
        comparable_keys = sorted(set(configs["before"]) | set(configs["after"]))
        for key in comparable_keys:
            if key in {"label", "revision", "lockfile_sha256"}:
                continue
            if configs["before"].get(key) != configs["after"].get(key):
                errors.append(f"workload provenance mismatch for {key!r}")
    for label, rows in by_label.items():
        directory = artifact / label
        config = configs[label]
        manifest_points = _configured_points(
            config, "manifest_points", (1, 10, 20, 40, 80, 160), label, errors
        )
        segment_points = _configured_points(config, "segment_points", EXPECTED_SEGMENTS, label, errors)
        lifecycle_pins = _configured_points(config, "lifecycle_pins", EXPECTED_PINS, label, errors)
        for benchmark in tuple(f"manifest_growth_{point}" for point in manifest_points) + (
            "segment_recall",
            "lifecycle",
        ):
            _validate_raw_provenance(directory, benchmark, config, label, errors)
            for suffix in (".log", ".time"):
                if not (directory / f"{benchmark}{suffix}").is_file():
                    errors.append(f"{label}: missing {benchmark}{suffix}")
            if not any(row["benchmark"] == benchmark for row in rows):
                errors.append(f"{label}: no parsed metrics for {benchmark}")
        for point in manifest_points:
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
            manifest_inputs = MANIFEST_INPUT.findall(manifest_text)
            expected_manifest = (
                point,
                int(config.get("manifest_warmup_runs", "0")),
                int(config.get("manifest_repetitions", "0")),
            )
            if manifest_inputs != [tuple(map(str, expected_manifest))]:
                errors.append(f"{label}: manifest_growth_{point} emitted protocol does not match config")
        segment_text = _read(directory / "segment_recall.log")
        expected_source = f"synthetic seed={config.get('seed', '')}"
        _validate_segment_log(
            segment_text, config, expected_source, label, "segment_recall", errors
        )
        _validate_segment_matrix(rows, label, "segment_recall", segment_points, errors)
        lifecycle_text = _read(directory / "lifecycle.log")
        loadings = LIFECYCLE_LOADING.findall(lifecycle_text)
        expected_loading = (
            config.get("lifecycle_rows", "0"),
            "512",
            expected_source,
        )
        expected_lifecycle_samples = (
            int(config.get("lifecycle_warmup_runs", "0"))
            + int(config.get("lifecycle_repetitions", "0"))
        ) * len(lifecycle_pins)
        if (
            len(loadings) != expected_lifecycle_samples
            or any(loading[:3] != expected_loading for loading in loadings)
            or len({loading[3] for loading in loadings}) != 1
        ):
            errors.append(f"{label}: lifecycle input metadata does not match config")
        commit_counts = [int(value) for value in LIFECYCLE_INGEST_COMMITS.findall(lifecycle_text)]
        lifecycle_rows_config = int(config.get("lifecycle_rows", "0"))
        batch_rows = int(config.get("lifecycle_batch_rows", "1"))
        expected_commits = (lifecycle_rows_config + batch_rows - 1) // batch_rows
        if not commit_counts or any(count != expected_commits for count in commit_counts):
            errors.append(f"{label}: lifecycle commit count does not match configured batch size")
        distinct_counts = [int(value) for _, value in LIFECYCLE_DISTINCT.findall(lifecycle_text)]
        if sorted(set(distinct_counts)) != list(lifecycle_pins):
            errors.append(f"{label}: lifecycle distinct-snapshot counts do not match configured pins")
        for pins in lifecycle_pins:
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
        for benchmark in tuple(f"manifest_growth_{point}" for point in manifest_points) + (
            "segment_recall",
            "lifecycle",
        ):
            if not any(row["benchmark"] == benchmark and row["max_rss_kb"] is not None for row in rows):
                errors.append(f"{label}: missing GNU time RSS metric for {benchmark}")
            _validate_single_rss(directory, benchmark, label, errors)
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
    fixture_lifecycle_configs = {
        label: _load_config(artifact / label, "fixture_lifecycle.env") for label in by_label
    }
    fixture_requested = {label for label, config in configs.items() if config.get("fixture_evidence") == "requested"}
    if fixture_requested and fixture_requested != {"before", "after"}:
        errors.append("fixture_evidence must be requested for both before and after")
    for label in by_label:
        directory = artifact / label
        requested = configs[label].get("fixture_evidence") == "requested"
        expected_status = "complete" if requested else "not-requested"
        if _exact_values(_read(directory / "fixture_segment_recall.status"), "fixture_status") != [expected_status]:
            errors.append(f"{label}: fixture_segment_recall.status must report {expected_status}")
        if _exact_values(_read(directory / "fixture_lifecycle.status"), "fixture_status") != [expected_status]:
            errors.append(f"{label}: fixture_lifecycle.status must report {expected_status}")
        paths = {
            "sidecar": directory / "fixture_segment_recall.env",
            "log": directory / "fixture_segment_recall.log",
            "time": directory / "fixture_segment_recall.time",
        }
        lifecycle_paths = {
            "sidecar": directory / "fixture_lifecycle.env",
            "log": directory / "fixture_lifecycle.log",
            "time": directory / "fixture_lifecycle.time",
        }
        if not requested:
            for name, path in {**paths, **{f"lifecycle {name}": value for name, value in lifecycle_paths.items()}}.items():
                if path.exists():
                    errors.append(f"{label}: synthetic-only evidence must not include fixture evidence {name}")
            continue
        missing = [name for name, path in paths.items() if not path.is_file()]
        if missing:
            errors.append(f"{label}: missing fixture_segment_recall {', '.join(missing)}")
            continue
        config = fixture_configs[label]
        sidecar_text = _read(paths["sidecar"])
        for key, value in config.items():
            if _exact_values(sidecar_text, key) != [value]:
                errors.append(f"{label}: fixture sidecar must emit {key} exactly once")
        if config.get("source") != "fixture":
            errors.append(f"{label}: fixture sidecar source must be fixture")
        else:
            try:
                validate_fixture_provenance(config)
            except ValueError as error:
                errors.append(f"{label}: {error}")
        for key in (
            "label",
            "revision",
            "lockfile_sha256",
            "segment_dimension",
            "segment_k",
            "segment_ef_search",
            "segment_m",
            "segment_ef_construction",
            "segment_max_layer",
            "segment_points",
            "segment_warmup_runs",
            "segment_repetitions",
        ):
            if config.get(key) != configs[label].get(key):
                errors.append(f"{label}: fixture sidecar {key} does not match config.env")
        for sidecar_key, selected_key in (
            ("segment_rows", "fixture_rows"),
            ("segment_queries", "fixture_queries"),
        ):
            if config.get(sidecar_key) != configs[label].get(selected_key):
                errors.append(
                    f"{label}: fixture sidecar {sidecar_key} does not match selected {selected_key}"
                )
        fixture_log = _read(paths["log"])
        for key in ("label", "revision", "lockfile_sha256", *FIXTURE_PROVENANCE, "fixture_worktree_path"):
            expected = config.get(key)
            if _exact_values(fixture_log, key) != [expected]:
                errors.append(f"{label}: fixture_segment_recall emitted {key} does not match its sidecar")
        expected_fixture_source = f"fixture {config.get('fixture_worktree_path', '')}"
        if config.get("fixture_source") != expected_fixture_source:
            errors.append(f"{label}: fixture_worktree_path does not match fixture_source")
        fixture_points = _configured_points(
            config, "segment_points", EXPECTED_SEGMENTS, label, errors
        )
        fixture_input_hash = config.get("fixture_input_hash")
        if not fixture_input_hash:
            errors.append(f"{label}: fixture_input_hash must be non-empty")
        else:
            _validate_segment_log(
                fixture_log,
                config,
                expected_fixture_source,
                label,
                "fixture_segment_recall",
                errors,
                fixture_input_hash,
            )
        _validate_segment_matrix(by_label[label], label, "fixture_segment_recall", fixture_points, errors)
        fixture_rows = [row for row in by_label[label] if row["benchmark"] == "fixture_segment_recall"]
        if not fixture_rows:
            errors.append(f"{label}: no parsed fixture_segment_recall metrics")
        elif any(row["max_rss_kb"] is None for row in fixture_rows):
            errors.append(f"{label}: missing fixture_segment_recall GNU time RSS metric")
        _validate_single_rss(directory, "fixture_segment_recall", label, errors)
        missing = [name for name, path in lifecycle_paths.items() if not path.is_file()]
        if missing:
            errors.append(f"{label}: missing fixture_lifecycle {', '.join(missing)}")
            continue
        config = fixture_lifecycle_configs[label]
        sidecar_text = _read(lifecycle_paths["sidecar"])
        for key, value in config.items():
            if _exact_values(sidecar_text, key) != [value]:
                errors.append(f"{label}: fixture lifecycle sidecar must emit {key} exactly once")
        if config.get("source") != "fixture":
            errors.append(f"{label}: fixture lifecycle sidecar source must be fixture")
        else:
            try:
                validate_fixture_provenance(config)
            except ValueError as error:
                errors.append(f"{label}: {error}")
        for key in ("label", "revision", "lockfile_sha256"):
            if config.get(key) != configs[label].get(key):
                errors.append(f"{label}: fixture lifecycle sidecar {key} does not match config.env")
        for sidecar_key, selected_key in (
            ("lifecycle_rows", "fixture_lifecycle_rows"),
            ("lifecycle_batch_rows", "fixture_lifecycle_batch_rows"),
            ("lifecycle_pins", "fixture_lifecycle_pins"),
            ("lifecycle_warmup_runs", "fixture_lifecycle_warmup_runs"),
            ("lifecycle_repetitions", "fixture_lifecycle_repetitions"),
            ("fixture_lifecycle_protocol", "fixture_lifecycle_protocol"),
        ):
            if config.get(sidecar_key) != configs[label].get(selected_key):
                errors.append(
                    f"{label}: fixture lifecycle sidecar {sidecar_key} does not match config.env"
                )
        fixture_log = _read(lifecycle_paths["log"])
        _validate_raw_provenance(directory, "fixture_lifecycle", config, label, errors)
        if _exact_values(fixture_log, "benchmark") != ["fixture_lifecycle"]:
            errors.append(f"{label}: fixture_lifecycle.log must identify its benchmark exactly once")
        for key in ("fixture_worktree_path", *FIXTURE_PROVENANCE):
            if _exact_values(fixture_log, key) != [config.get(key)]:
                errors.append(f"{label}: fixture_lifecycle emitted {key} does not match its sidecar")
        expected_fixture_source = f"fixture {config.get('fixture_worktree_path', '')}"
        if config.get("fixture_source") != expected_fixture_source:
            errors.append(f"{label}: fixture lifecycle worktree path does not match fixture_source")
        fixture_input_hash = config.get("fixture_input_hash")
        if not fixture_input_hash:
            errors.append(f"{label}: fixture lifecycle input hash must be non-empty")
            continue
        try:
            fixture_pins = tuple(int(value) for value in config.get("lifecycle_pins", "").split(","))
        except ValueError:
            fixture_pins = ()
        if fixture_pins != (1,):
            errors.append(f"{label}: fixture lifecycle pins must equal 1")
        expected_loading = (config.get("lifecycle_rows", "0"), "512", expected_fixture_source)
        loadings = LIFECYCLE_LOADING.findall(fixture_log)
        expected_samples = (
            int(config.get("lifecycle_warmup_runs", "0"))
            + int(config.get("lifecycle_repetitions", "0"))
        ) * len(fixture_pins)
        if (
            len(loadings) != expected_samples
            or any(loading[:3] != expected_loading or loading[3] != fixture_input_hash for loading in loadings)
        ):
            errors.append(f"{label}: fixture lifecycle input metadata does not match sidecar")
        lifecycle_blocks = [
            block
            for block in re.split(r"(?=^================ StrataDB lifecycle)", fixture_log, flags=re.MULTILINE)
            if re.search(r"^================ StrataDB lifecycle", block, flags=re.MULTILINE)
        ]
        measurements: list[str] = []
        for block in lifecycle_blocks:
            markers = LIFECYCLE_MEASUREMENT.findall(block)
            if len(markers) != 1:
                errors.append(f"{label}: each fixture lifecycle block must contain exactly one measurement marker")
                continue
            measurements.append(markers[0].strip())
        if tuple(measurements) != EXPECTED_FIXTURE_LIFECYCLE_MEASUREMENTS:
            errors.append(f"{label}: fixture lifecycle measurement protocol does not match the exact warmup/repetition sequence")
        for block in lifecycle_blocks:
            phases = [_lifecycle_phase_name(match) for match in LIFECYCLE_PHASE.finditer(block)]
            phase_counts = Counter(phases)
            expected_phase_counts = Counter(EXPECTED_LIFECYCLE_PHASES)
            if phase_counts != expected_phase_counts:
                missing_phases = sorted(expected_phase_counts - phase_counts)
                unexpected_phases = sorted(phase_counts - expected_phase_counts)
                errors.append(
                    f"{label}: fixture lifecycle block phases must match the exact emitted set once; "
                    f"missing={','.join(missing_phases) or 'none'}; "
                    f"unexpected-or-duplicate={','.join(unexpected_phases) or 'none'}"
                )
        commit_counts = [int(value) for value in LIFECYCLE_INGEST_COMMITS.findall(fixture_log)]
        rows_config = int(config.get("lifecycle_rows", "0"))
        batch_rows = int(config.get("lifecycle_batch_rows", "1"))
        expected_commits = (rows_config + batch_rows - 1) // batch_rows
        if not commit_counts or any(count != expected_commits for count in commit_counts):
            errors.append(f"{label}: fixture lifecycle commit count does not match configured batch size")
        distinct_counts = [int(value) for _, value in LIFECYCLE_DISTINCT.findall(fixture_log)]
        if sorted(set(distinct_counts)) != list(fixture_pins):
            errors.append(f"{label}: fixture lifecycle distinct-snapshot counts do not match configured pins")
        fixture_lifecycle_rows = [row for row in by_label[label] if row["benchmark"] == "fixture_lifecycle"]
        metric = "lifecycle_pins1_ingest_commit_wall_ms"
        metric_rows = [row for row in fixture_lifecycle_rows if row["metric"] == metric]
        if not metric_rows:
            errors.append(f"{label}: missing fixture_lifecycle/{metric}")
        elif any(row.get("samples") != int(config.get("lifecycle_repetitions", "0")) for row in metric_rows):
            errors.append(f"{label}: fixture lifecycle ingest metrics do not contain measured repetitions")
        hashes = {tuple(row.get("input_hashes", [])) for row in fixture_lifecycle_rows}
        if hashes != {(fixture_input_hash,)}:
            errors.append(f"{label}: fixture lifecycle must contain exactly its sidecar input hash")
        if any(row["max_rss_kb"] is None for row in fixture_lifecycle_rows):
            errors.append(f"{label}: missing fixture_lifecycle GNU time RSS metric")
        _validate_single_rss(directory, "fixture_lifecycle", label, errors)
    if fixture_requested == {"before", "after"}:
        before_hash = fixture_configs["before"].get("fixture_input_hash")
        after_hash = fixture_configs["after"].get("fixture_input_hash")
        if before_hash and after_hash and before_hash != after_hash:
            errors.append("before/after fixture_segment_recall input hashes differ")
        before_lifecycle_hash = fixture_lifecycle_configs["before"].get("fixture_input_hash")
        after_lifecycle_hash = fixture_lifecycle_configs["after"].get("fixture_input_hash")
        if (
            before_lifecycle_hash
            and after_lifecycle_hash
            and before_lifecycle_hash != after_lifecycle_hash
        ):
            errors.append("before/after fixture_lifecycle input hashes differ")
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
