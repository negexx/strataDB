import tempfile
import unittest
import subprocess
import sys
import re
from pathlib import Path

import summarize


class SummarizeTests(unittest.TestCase):
    LIFECYCLE_PHASE_LINES = (
        "ingest+commit                    20.00ms       2.0MB       3.0MB      -1.0MB  fsync",
        "recovery (reopen)                20.00ms       2.0MB       3.0MB      -1.0MB  I/O + validation",
        "pinned snapshot/cache residency  20.00ms       2.0MB       3.0MB      -1.0MB  cache accounting",
        "full scan                        20.00ms       2.0MB       3.0MB      -1.0MB  I/O",
        "filtered scan                    20.00ms       2.0MB       3.0MB      -1.0MB  I/O + CPU",
        "group-by (4 aggs)                20.00ms       2.0MB       3.0MB      -1.0MB  CPU",
        "vector search (unfiltered)       20.00ms       2.0MB       3.0MB      -1.0MB  CPU",
        "vector search (filtered)         20.00ms       2.0MB       3.0MB      -1.0MB  I/O + CPU",
        "vector search (filtered, varying predicate) 20.00ms       2.0MB       3.0MB      -1.0MB  I/O + CPU",
        "concurrent commits                20.00ms       2.0MB       3.0MB      -1.0MB  I/O",
    )

    def _write_complete_configured_matrix(self, artifact: Path, fixture_evidence: str) -> None:
        config = f"""workload_signature=synthetic-seed-20260801-dim512-hnsw-M16-efc100-efsearch32-k10
seed=20260801
source=synthetic
fixture_evidence={fixture_evidence}
fixture_rows=256
fixture_queries=16
manifest_points=1,10,20,40,80,160
manifest_warmup_runs=1
manifest_repetitions=5
segment_rows=256
segment_queries=16
segment_dimension=512
segment_k=10
segment_ef_search=32
segment_m=16
segment_ef_construction=100
segment_max_layer=16
segment_points=1,2,4,8,16,32,64
segment_warmup_runs=1
segment_repetitions=5
lifecycle_rows=64
lifecycle_batch_rows=1
lifecycle_pins=0,1,4,16,64
lifecycle_warmup_runs=1
lifecycle_repetitions=5
fixture_lifecycle_rows=100000
fixture_lifecycle_batch_rows=5000
fixture_lifecycle_pins=1
fixture_lifecycle_warmup_runs=1
fixture_lifecycle_repetitions=5
fixture_lifecycle_protocol=fixture-100000-rows-batch-5000-pins-1-warmups-1-repetitions-5
command_manifest=manifest
command_segment=segment
command_lifecycle=lifecycle
"""
        for label in ("before", "after"):
            directory = artifact / label
            directory.mkdir()
            raw_headers = [
                f"label={label}",
                f"revision={'a' if label == 'before' else 'b'}",
                "lockfile_sha256=lock",
            ]
            (directory / "config.env").write_text(
                f"label={label}\nrevision={'a' if label == 'before' else 'b'}\n"
                "lockfile_sha256=lock\n" + config,
                encoding="utf-8",
            )
            for point in (1, 10, 20, 40, 80, 160):
                (directory / f"manifest_growth_{point}.log").write_text(
                    "\n".join(raw_headers)
                    + "\n"
                    + f"manifest growth â€” {point} sequential commits, one data file each\n"
                    f"input: deterministic id-only rows; commits={point}; buckets=20; warmup runs excluded=1; measured repetitions=5\n"
                    "median commit-sequence wall: 1.000 ms; p95: 1.100 ms; sample variance: 0.100 ms^2\n"
                    "median newest manifest: 712 bytes; p95: 713 bytes; sample variance: 0.100 bytes^2\n",
                    encoding="utf-8",
                )
                (directory / f"manifest_growth_{point}.time").write_text(
                    "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
                )
            segment = [
                *raw_headers,
                "loaded 256 rows from synthetic seed=20260801; input hash=abc123",
                "computing exact ground truth for 16 queries...",
                "==== recall vs segment count â€” 256 rows x 512-dim, k=10, ef_search=32 ====",
                "production HNSW parameters: M=16, ef_construction=100, max_layer=16",
                "query policy: 1 full unfiltered+filtered warmup sweep(s), then 5 measured sweep(s) per K",
            ]
            for mode in ("unfiltered", "filtered"):
                segment.append(f"{mode} query results (median / p95 over measured repetitions):")
                for point in (1, 2, 4, 8, 16, 32, 64):
                    segment.append(f"{point:4d}  1.0000 / 1.0000     10.0 / 11.0     100 / 90")
            (directory / "segment_recall.log").write_text("\n".join(segment), encoding="utf-8")
            (directory / "segment_recall.time").write_text(
                "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
            )
            lifecycle = [*raw_headers]
            for pins in (0, 1, 4, 16, 64):
                distinct_snapshots = 0 if pins == 0 else 1
                lifecycle.extend(
                    [
                        "================ StrataDB lifecycle Ã¢â‚¬â€ 64 rows x 512-dim ================",
                        "ingest+commit                    20.00ms       2.0MB       3.0MB      -1.0MB  fsync",
                        "                            64 commits, 64 rows/s, 1.00 ms/commit",
                        "8 threads x 3 = 24 commits, 24/s",
                        "input hash=abc123",
                        "measurement             : excluded warmup 1/1; pins=" + str(pins),
                        f"{pins} retained handles ({distinct_snapshots} distinct snapshots; operational current snapshot excluded);",
                        "loading 64 rows (512-dim) from synthetic seed=20260801; input hash=abc123",
                        "newest manifest bytes  : 200",
                    ]
                )
                for repetition in range(1, 6):
                    lifecycle.extend(
                        [
                            "================ StrataDB lifecycle â€” 64 rows x 512-dim ================",
                            "ingest+commit                    20.00ms       2.0MB       3.0MB      -1.0MB  fsync",
                            "                            64 commits, 64 rows/s, 1.00 ms/commit",
                            "8 threads x 3 = 24 commits, 24/s",
                            "input hash=abc123",
                            f"measurement             : measured repetition {repetition}/5; pins={pins}",
                            f"{pins} retained handles ({distinct_snapshots} distinct snapshots; operational current snapshot excluded);",
                            "loading 64 rows (512-dim) from synthetic seed=20260801; input hash=abc123",
                            "newest manifest bytes  : 200",
                        ]
                    )
            (directory / "lifecycle.log").write_text("\n".join(lifecycle), encoding="utf-8")
            (directory / "lifecycle.time").write_text(
                "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
            )
            if fixture_evidence == "not-requested":
                (directory / "fixture_segment_recall.status").write_text(
                    "fixture_status=not-requested\n", encoding="utf-8"
                )
                (directory / "fixture_lifecycle.status").write_text(
                    "fixture_status=not-requested\n", encoding="utf-8"
                )

    def _write_fixture_lifecycle_evidence(
        self,
        artifact: Path,
        label: str,
        *,
        path: str,
        input_hash: str,
    ) -> None:
        directory = artifact / label
        config = {
            "label": label,
            "revision": "a" if label == "before" else "b",
            "lockfile_sha256": "lock",
            "source": "fixture",
            **summarize.FIXTURE_PROVENANCE,
            "fixture_worktree_path": path,
            "fixture_source": f"fixture {path}",
            "fixture_input_hash": input_hash,
            "lifecycle_rows": "100000",
            "lifecycle_batch_rows": "5000",
            "lifecycle_pins": "1",
            "lifecycle_warmup_runs": "1",
            "lifecycle_repetitions": "5",
            "fixture_lifecycle_protocol": "fixture-100000-rows-batch-5000-pins-1-warmups-1-repetitions-5",
            "command": "fixture-lifecycle-command",
        }
        (directory / "fixture_lifecycle.env").write_text(
            "".join(f"{key}={value}\n" for key, value in config.items()), encoding="utf-8"
        )
        log = [
            f"label={label}",
            f"revision={config['revision']}",
            "lockfile_sha256=lock",
            "benchmark=fixture_lifecycle",
            *(f"{key}={value}" for key, value in summarize.FIXTURE_PROVENANCE.items()),
            f"fixture_worktree_path={path}",
            f"command={config['command']}",
        ]
        for measurement in ("excluded warmup 1/1; pins=1", *(f"measured repetition {i}/5; pins=1" for i in range(1, 6))):
            log.extend(
                [
                    "================ StrataDB lifecycle â€” 100000 rows x 512-dim ================",
                    "ingest+commit                    20.00ms       2.0MB       3.0MB      -1.0MB  fsync",
                    "recovery (reopen)                10.00ms       1.0MB       2.0MB      -0.5MB  I/O + validation",
                    "compaction+reclaim               12.00ms       3.0MB       4.0MB      -0.5MB  rewrite + reclamation",
                    "bounded lifecycle maintenance     6.00ms       1.5MB       2.5MB      -0.5MB  maintenance",
                    "pinned snapshot/cache residency   1.00ms       0.1MB       0.2MB      -0.1MB  direct retained payloads",
                    "full scan                          5.00ms       1.0MB       1.5MB      -0.2MB  I/O",
                    "filtered scan                      2.00ms       0.2MB       0.3MB      -0.1MB  I/O + CPU",
                    "group-by (4 aggs)                  1.00ms       0.1MB       0.2MB      -0.1MB  CPU",
                    "vector search (unfiltered)         3.00ms       0.2MB       0.3MB      -0.1MB  CPU",
                    "vector search (filtered)           4.00ms       0.2MB       0.3MB      -0.1MB  I/O-bound",
                    "vector search (filtered, varying predicate) 4.00ms       0.2MB       0.3MB      -0.1MB  I/O-bound",
                    "concurrent commits                  8.00ms       0.3MB       0.4MB      -0.1MB  I/O-bound",
                    "                            20 commits, 5000 rows/s, 1.00 ms/commit",
                    "1 retained handles (1 distinct snapshots; operational current snapshot excluded);",
                    f"loading 100000 rows (512-dim) from fixture {path}; input hash={input_hash}",
                    f"measurement             : {measurement}",
                    "newest manifest bytes  : 200",
                ]
            )
        (directory / "fixture_lifecycle.log").write_text("\n".join(log), encoding="utf-8")
        (directory / "fixture_lifecycle.time").write_text(
            "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
        )
        (directory / "fixture_lifecycle.status").write_text(
            "fixture_status=complete\n", encoding="utf-8"
        )

    def _write_fixture_evidence(
        self,
        artifact: Path,
        label: str,
        *,
        path: str | None = None,
        input_hash: str = "f1a2ce123",
        rows: str = "256",
        queries: str = "16",
        include_lifecycle: bool = True,
    ) -> None:
        directory = artifact / label
        fixture_path = path or f"/worktrees/{label}/bench/data/dbpedia-openai-100k.parquet"
        config = {
            "label": label,
            "revision": "a" if label == "before" else "b",
            "lockfile_sha256": "lock",
            "source": "fixture",
            **summarize.FIXTURE_PROVENANCE,
            "fixture_worktree_path": fixture_path,
            "fixture_source": f"fixture {fixture_path}",
            "fixture_input_hash": input_hash,
            "segment_rows": rows,
            "segment_queries": queries,
            "segment_dimension": "512",
            "segment_k": "10",
            "segment_ef_search": "32",
            "segment_m": "16",
            "segment_ef_construction": "100",
            "segment_max_layer": "16",
            "segment_points": "1,2,4,8,16,32,64",
            "segment_warmup_runs": "1",
            "segment_repetitions": "5",
        }
        (directory / "fixture_segment_recall.env").write_text(
            "".join(f"{key}={value}\n" for key, value in config.items()), encoding="utf-8"
        )
        log = [
            f"label={label}",
            f"revision={config['revision']}",
            "lockfile_sha256=lock",
            "benchmark=fixture_segment_recall",
            *(f"{key}={value}" for key, value in summarize.FIXTURE_PROVENANCE.items()),
            f"fixture_worktree_path={fixture_path}",
            f"loaded {rows} rows from fixture {fixture_path}; input hash={input_hash}",
            f"computing exact ground truth for {queries} queries...",
            "==== recall vs segment count â€” 256 rows x 512-dim, k=10, ef_search=32 ====",
            "production HNSW parameters: M=16, ef_construction=100, max_layer=16",
            "query policy: 1 full unfiltered+filtered warmup sweep(s), then 5 measured sweep(s) per K",
        ]
        log = [
            line.replace("256 rows x 512-dim", f"{rows} rows x 512-dim") for line in log
        ]
        for mode in ("unfiltered", "filtered"):
            log.append(f"{mode} query results (median / p95 over measured repetitions):")
            for point in (1, 2, 4, 8, 16, 32, 64):
                log.append(f"{point:4d}  1.0000 / 1.0000     10.0 / 11.0     100 / 90")
        (directory / "fixture_segment_recall.log").write_text("\n".join(log), encoding="utf-8")
        (directory / "fixture_segment_recall.time").write_text(
            "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
        )
        (directory / "fixture_segment_recall.status").write_text(
            "fixture_status=complete\n", encoding="utf-8"
        )
        if include_lifecycle:
            self._write_fixture_lifecycle_evidence(
                artifact, label, path=fixture_path, input_hash=input_hash
            )

    def test_fixture_provenance_rejects_missing_pinned_sha256(self):
        config = {
            "source": "fixture",
            "fixture_repo": "Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K",
            "fixture_revision": "56e6849a3d0f7913e56b475bf92c0064c93b576d",
            "fixture_file": "data/train-00000-of-00001.parquet",
            "fixture_size_bytes": "363758493",
        }
        with self.assertRaisesRegex(ValueError, "fixture_sha256"):
            summarize.validate_fixture_provenance(config)

    def test_collects_fixture_segment_metrics_with_pinned_provenance(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "before"
            directory.mkdir()
            config = {
                "label": "before",
                "revision": "revision",
                "lockfile_sha256": "lock",
                "source": "fixture",
                **summarize.FIXTURE_PROVENANCE,
            }
            (directory / "fixture_segment_recall.env").write_text(
                "".join(f"{key}={value}\n" for key, value in config.items()), encoding="utf-8"
            )
            (directory / "fixture_segment_recall.log").write_text(
                "loaded 64 rows from fixture /tmp/dbpedia-openai-100k.parquet; input hash=abc123\n"
                "unfiltered query results (median / p95 over measured repetitions):\n"
                "   1   1.0000 /  1.0000     10.0 /    11.0    100 /   90\n",
                encoding="utf-8",
            )
            (directory / "fixture_segment_recall.time").write_text(
                "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
            )

            records = summarize.summarize_directory(Path(root))

            self.assertTrue(records)
            self.assertEqual({row["benchmark"] for row in records}, {"fixture_segment_recall"})
            self.assertTrue(all(row["config"]["source"] == "fixture" for row in records))

    def test_collects_manifest_and_segment_metrics_and_delta(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            (artifact / "before").mkdir()
            (artifact / "after").mkdir()
            for label, wall, latency in (("before", "100.000", "10.0"), ("after", "80.000", "8.0")):
                (artifact / label / "manifest_growth.log").write_text(
                    f"median commit-sequence wall: {wall} ms; p95: {wall} ms\n"
                    "median newest manifest: 712 bytes; p95: 712 bytes\n",
                    encoding="utf-8",
                )
                (artifact / label / "segment_recall.log").write_text(
                    "unfiltered query results (median / p95 over measured repetitions):\n"
                    f"   1   1.0000 /  1.0000     {latency} /    {latency}    100 /   100\n",
                    encoding="utf-8",
                )
                (artifact / label / "manifest_growth.time").write_text(
                    "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
                )

            records = summarize.summarize_directory(artifact)
            deltas = summarize.compute_deltas(records)

            manifest = next(
                row for row in records if row["label"] == "after" and row["metric"] == "wall_ms"
            )
            self.assertEqual(manifest["value"], 80.0)
            self.assertEqual(manifest["max_rss_kb"], 1234)
            latency_delta = next(
                row for row in deltas if row["metric"] == "segment_k1_unfiltered_us_per_query_median"
            )
            self.assertEqual(latency_delta["delta"], -2.0)
            self.assertEqual(latency_delta["relative_delta"], -0.2)

    def test_lifecycle_ignores_warmup_and_summarizes_measured_phase(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "before"
            directory.mkdir()
            block = """
================ StrataDB lifecycle — 4 rows x 512-dim ================
ingest+commit                    10.00ms       1.0MB       2.0MB       0.0MB  fsync
measurement             : excluded warmup 1/1; pins=0
newest manifest bytes  : 100
================ StrataDB lifecycle — 4 rows x 512-dim ================
ingest+commit                    20.00ms       2.0MB       3.0MB      -1.0MB  fsync
measurement             : measured repetition 1/5; pins=0
newest manifest bytes  : 200
"""
            (directory / "lifecycle.log").write_text(block, encoding="utf-8")
            (directory / "lifecycle.time").write_text(
                "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
            )
            rows = summarize._lifecycle("before", directory, {})
            wall = next(row for row in rows if row["metric"] == "lifecycle_pins0_ingest_commit_wall_ms")
            self.assertEqual(wall["value"], 20.0)
            self.assertEqual(wall["samples"], 1)
            live = next(row for row in rows if row["metric"] == "lifecycle_pins0_ingest_commit_live_delta_mb")
            self.assertEqual(live["value"], -1.0)

    def test_validation_rejects_an_incomplete_matrix(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            records = [
                {
                    "label": "before",
                    "benchmark": "manifest_growth",
                    "metric": "wall_ms",
                    "value": 1.0,
                    "max_rss_kb": 1,
                }
            ]
            with self.assertRaises(ValueError):
                summarize.validate_records(records, artifact)

    def test_cli_writes_machine_readable_outputs(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root) / "before"
            artifact.mkdir()
            (artifact / "manifest_growth.log").write_text(
                "median commit-sequence wall: 1.000 ms; p95: 1.000 ms\n"
                "median newest manifest: 712 bytes; p95: 712 bytes\n",
                encoding="utf-8",
            )
            jsonl = Path(root) / "summary.jsonl"
            csv = Path(root) / "deltas.csv"
            result = subprocess.run(
                [sys.executable, str(Path(summarize.__file__)), str(Path(root)), "--jsonl", str(jsonl), "--csv", str(csv)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(jsonl.is_file())
            self.assertTrue(csv.is_file())

    def test_validation_accepts_the_complete_configured_matrix(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            config = """workload_signature=synthetic-seed-20260801-dim512-hnsw-M16-efc100-efsearch32-k10
seed=20260801
source=synthetic
fixture_evidence=not-requested
manifest_points=1,10,20,40,80,160
manifest_warmup_runs=1
manifest_repetitions=5
segment_rows=256
segment_queries=16
segment_dimension=512
segment_k=10
segment_ef_search=32
segment_m=16
segment_ef_construction=100
segment_max_layer=16
segment_points=1,2,4,8,16,32,64
segment_warmup_runs=1
segment_repetitions=5
lifecycle_rows=64
lifecycle_batch_rows=1
lifecycle_pins=0,1,4,16,64
lifecycle_warmup_runs=1
lifecycle_repetitions=5
fixture_lifecycle_rows=100000
fixture_lifecycle_batch_rows=5000
fixture_lifecycle_pins=1
fixture_lifecycle_warmup_runs=1
fixture_lifecycle_repetitions=5
fixture_lifecycle_protocol=fixture-100000-rows-batch-5000-pins-1-warmups-1-repetitions-5
command_manifest=manifest
command_segment=segment
command_lifecycle=lifecycle
"""
            for label in ("before", "after"):
                directory = artifact / label
                directory.mkdir()
                raw_headers = [
                    f"label={label}",
                    f"revision={'a' if label == 'before' else 'b'}",
                    "lockfile_sha256=lock",
                ]
                (directory / "fixture_segment_recall.status").write_text(
                    "fixture_status=not-requested\n", encoding="utf-8"
                )
                (directory / "fixture_lifecycle.status").write_text(
                    "fixture_status=not-requested\n", encoding="utf-8"
                )
                (directory / "config.env").write_text(
                    f"label={label}\nrevision={'a' if label == 'before' else 'b'}\n"
                    "lockfile_sha256=lock\n" + config,
                    encoding="utf-8",
                )
                for point in (1, 10, 20, 40, 80, 160):
                    (directory / f"manifest_growth_{point}.log").write_text(
                        "\n".join(raw_headers)
                        + "\n"
                        + f"manifest growth — {point} sequential commits, one data file each\n"
                        f"input: deterministic id-only rows; commits={point}; buckets=20; warmup runs excluded=1; measured repetitions=5\n"
                        "median commit-sequence wall: 1.000 ms; p95: 1.100 ms; sample variance: 0.100 ms^2\n"
                        "median newest manifest: 712 bytes; p95: 713 bytes; sample variance: 0.100 bytes^2\n",
                        encoding="utf-8",
                    )
                    (directory / f"manifest_growth_{point}.time").write_text(
                        "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
                    )
                segment = [
                    *raw_headers,
                    "loaded 256 rows from synthetic seed=20260801; input hash=abc123",
                    "computing exact ground truth for 16 queries...",
                    "==== recall vs segment count — 256 rows x 512-dim, k=10, ef_search=32 ====",
                    "production HNSW parameters: M=16, ef_construction=100, max_layer=16",
                    "query policy: 1 full unfiltered+filtered warmup sweep(s), then 5 measured sweep(s) per K",
                ]
                for mode in ("unfiltered", "filtered"):
                    segment.append(f"{mode} query results (median / p95 over measured repetitions):")
                    for point in (1, 2, 4, 8, 16, 32, 64):
                        segment.append(f"{point:4d}  1.0000 / 1.0000     10.0 / 11.0     100 / 90")
                (directory / "segment_recall.log").write_text("\n".join(segment), encoding="utf-8")
                (directory / "segment_recall.time").write_text(
                    "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
                )
                lifecycle = [*raw_headers]
                for pins in (0, 1, 4, 16, 64):
                    lifecycle.extend(
                        [
                            "================ StrataDB lifecycle Ã¢â‚¬â€ 64 rows x 512-dim ================",
                            "ingest+commit                    20.00ms       2.0MB       3.0MB      -1.0MB  fsync",
                            "                            64 commits, 64 rows/s, 1.00 ms/commit",
                            "8 threads x 3 = 24 commits, 24/s",
                            "input hash=abc123",
                            f"measurement             : excluded warmup 1/1; pins={pins}",
                            f"{pins} retained handles ({pins} distinct snapshots; operational current snapshot excluded);",
                            "loading 64 rows (512-dim) from synthetic seed=20260801; input hash=abc123",
                            "newest manifest bytes  : 200",
                        ]
                    )
                    for repetition in range(1, 6):
                        lifecycle.extend(
                            [
                                "================ StrataDB lifecycle — 64 rows x 512-dim ================",
                                "ingest+commit                    20.00ms       2.0MB       3.0MB      -1.0MB  fsync",
                                "                            64 commits, 64 rows/s, 1.00 ms/commit",
                                "8 threads x 3 = 24 commits, 24/s",
                                "input hash=abc123",
                                f"measurement             : measured repetition {repetition}/5; pins={pins}",
                                f"{pins} retained handles ({pins} distinct snapshots; operational current snapshot excluded);",
                                "loading 64 rows (512-dim) from synthetic seed=20260801; input hash=abc123",
                                "newest manifest bytes  : 200",
                            ]
                        )
                (directory / "lifecycle.log").write_text("\n".join(lifecycle), encoding="utf-8")
                (directory / "lifecycle.time").write_text(
                    "Maximum resident set size (kbytes): 1234\n", encoding="utf-8"
                )
            records = summarize.summarize_directory(artifact)
            summarize.validate_records(records, artifact)
            self.assertTrue(summarize.compute_deltas(records))
            before_lifecycle = artifact / "before" / "lifecycle.log"
            before_lifecycle.write_text(
                before_lifecycle.read_text(encoding="utf-8").replace(
                    "input hash=abc123", "input hash=abc123\ninput hash=def456", 1
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_forged_synthetic_raw_provenance(self):
        for key, replacement in (
            ("label", "forged-label"),
            ("revision", "forged-revision"),
            ("lockfile_sha256", "forged-lockfile"),
        ):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as root:
                artifact = Path(root)
                self._write_complete_configured_matrix(artifact, "not-requested")
                log = artifact / "before" / "segment_recall.log"
                expected = "before" if key == "label" else "a" if key == "revision" else "lock"
                log.write_text(
                    log.read_text(encoding="utf-8").replace(
                        f"{key}={expected}\n", f"{key}={replacement}\n", 1
                    ),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(
                    ValueError,
                    rf"before: segment_recall\.log must emit {key} exactly once matching config\.env",
                ):
                    summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_missing_or_duplicate_synthetic_raw_provenance_headers(self):
        for key in ("label", "revision", "lockfile_sha256"):
            for mutation in ("missing", "duplicate"):
                with self.subTest(key=key, mutation=mutation), tempfile.TemporaryDirectory() as root:
                    artifact = Path(root)
                    self._write_complete_configured_matrix(artifact, "not-requested")
                    log = artifact / "before" / "manifest_growth_1.log"
                    expected = "before" if key == "label" else "a" if key == "revision" else "lock"
                    text = log.read_text(encoding="utf-8")
                    if mutation == "missing":
                        text = text.replace(f"{key}={expected}\n", "", 1)
                    else:
                        text += f"\n{key}={expected}\n"
                    log.write_text(text, encoding="utf-8")

                    with self.assertRaisesRegex(
                        ValueError,
                        rf"before: manifest_growth_1\.log must emit {key} exactly once matching config\.env",
                    ):
                        summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_accepts_complete_before_after_fixture_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)

            records = summarize.summarize_directory(artifact)

            summarize.validate_records(records, artifact)
            self.assertTrue(
                any(row["benchmark"] == "fixture_segment_recall" for row in records)
            )

    def test_validation_rejects_fixture_lifecycle_without_compaction_and_maintenance(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            path = artifact / "before" / "fixture_lifecycle.log"
            path.write_text(
                "\n".join(
                    line
                    for line in path.read_text(encoding="utf-8").splitlines()
                    if not line.startswith(("compaction+reclaim", "bounded lifecycle maintenance"))
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "exact emitted set"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_applies_configured_fixture_lifecycle_regression_budget(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
                for filename in ("config.env", "fixture_lifecycle.env"):
                    path = artifact / label / filename
                    path.write_text(
                        path.read_text(encoding="utf-8")
                        + "fixture_lifecycle_max_regression_pct=10\n",
                        encoding="utf-8",
                    )
            after_log = artifact / "after" / "fixture_lifecycle.log"
            after_log.write_text(
                after_log.read_text(encoding="utf-8").replace(
                    "compaction+reclaim               12.00ms",
                    "compaction+reclaim               14.00ms",
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "regression budget"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_requested_fixture_without_lifecycle_artifacts(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label, include_lifecycle=False)

            with self.assertRaisesRegex(ValueError, "fixture_lifecycle"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_fixture_lifecycle_hash_mismatch_between_revisions(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            after = artifact / "after"
            for filename in ("fixture_lifecycle.env", "fixture_lifecycle.log"):
                path = after / filename
                path.write_text(
                    path.read_text(encoding="utf-8").replace("f1a2ce123", "d4e5fa678"),
                    encoding="utf-8",
                )

            with self.assertRaisesRegex(
                ValueError, "before/after fixture_lifecycle input hashes differ"
            ):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_fixture_lifecycle_measurement_sequence_mismatch(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            path = artifact / "before" / "fixture_lifecycle.log"
            path.write_text(
                path.read_text(encoding="utf-8").replace(
                    "measured repetition 2/5; pins=1", "measured repetition 1/5; pins=1", 1
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "measurement protocol"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_fixture_lifecycle_block_metadata_deletion_duplication_or_mismatch(self):
        metadata = {
            "commit": (
                "                            20 commits, 5000 rows/s, 1.00 ms/commit",
                "                            19 commits, 5000 rows/s, 1.00 ms/commit",
            ),
            "distinct snapshots": (
                "1 retained handles (1 distinct snapshots; operational current snapshot excluded);",
                "2 retained handles (2 distinct snapshots; operational current snapshot excluded);",
            ),
        }
        for name, (expected, mismatch) in metadata.items():
            for mutation in ("deleted", "duplicated", "mismatched"):
                with self.subTest(metadata=name, mutation=mutation), tempfile.TemporaryDirectory() as root:
                    artifact = Path(root)
                    self._write_complete_configured_matrix(artifact, "requested")
                    for label in ("before", "after"):
                        self._write_fixture_evidence(artifact, label)
                    path = artifact / "before" / "fixture_lifecycle.log"
                    text = path.read_text(encoding="utf-8")
                    if mutation == "deleted":
                        text = text.replace(f"{expected}\n", "", 1)
                    elif mutation == "duplicated":
                        text = text.replace(expected, f"{expected}\n{expected}", 1)
                    else:
                        text = text.replace(expected, mismatch, 1)
                    path.write_text(text, encoding="utf-8")

                    with self.assertRaisesRegex(ValueError, "fixture lifecycle block metadata"):
                        summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_missing_fixture_lifecycle_command_provenance(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            path = artifact / "before" / "fixture_lifecycle.log"
            path.write_text(
                path.read_text(encoding="utf-8").replace(
                    "command=fixture-lifecycle-command\n", "", 1
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "command provenance"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_truncated_fixture_lifecycle_phases(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            path = artifact / "before" / "fixture_lifecycle.log"
            lines = [
                line
                for line in path.read_text(encoding="utf-8").splitlines()
                if not line.startswith("recovery (reopen)")
            ]
            path.write_text("\n".join(lines), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exact emitted set"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_duplicate_fixture_lifecycle_phase(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            path = artifact / "before" / "fixture_lifecycle.log"
            text = path.read_text(encoding="utf-8")
            duplicate = "recovery (reopen)                10.00ms       1.0MB       2.0MB      -0.5MB  I/O + validation"
            path.write_text(text.replace(duplicate, f"{duplicate}\n{duplicate}", 1), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exact emitted set"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_unexpected_fixture_lifecycle_phase(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            path = artifact / "before" / "fixture_lifecycle.log"
            text = path.read_text(encoding="utf-8")
            extra = "unexpected phase                    1.00ms       0.1MB       0.2MB      -0.1MB  forged"
            path.write_text(text.replace("newest manifest bytes  : 200", f"{extra}\nnewest manifest bytes  : 200", 1), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exact emitted set"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_duplicate_fixture_lifecycle_measurement_marker(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            path = artifact / "before" / "fixture_lifecycle.log"
            text = path.read_text(encoding="utf-8")
            marker = "measurement             : excluded warmup 1/1; pins=1"
            path.write_text(text.replace(marker, f"{marker}\n{marker}", 1), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exactly one measurement marker"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_empty_fixture_lifecycle_block(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            path = artifact / "before" / "fixture_lifecycle.log"
            text = path.read_text(encoding="utf-8")
            header = "================ StrataDB lifecycle — forged empty block ================"
            path.write_text(f"{text}\n{header}\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exactly one measurement marker"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_summarization_rejects_empty_measured_fixture_lifecycle_block(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            path = artifact / "before" / "fixture_lifecycle.log"
            text = path.read_text(encoding="utf-8")
            forged = (
                "================ StrataDB lifecycle — forged measured block ================\n"
                "measurement             : measured repetition 6/5; pins=1\n"
            )
            path.write_text(f"{text}\n{forged}", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exact emitted set"):
                summarize.summarize_directory(artifact)

    def test_validation_accepts_selected_full_fixture_for_both_revisions(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                config = artifact / label / "config.env"
                config.write_text(
                    config.read_text(encoding="utf-8").replace(
                        "fixture_rows=256\nfixture_queries=16\n",
                        "fixture_rows=100000\nfixture_queries=200\n",
                        1,
                    ),
                    encoding="utf-8",
                )
                self._write_fixture_evidence(
                    artifact, label, rows="100000", queries="200"
                )

            summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_fixture_input_hash_mismatch_between_revisions(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            self._write_fixture_evidence(artifact, "before", input_hash="f1a2ce123")
            self._write_fixture_evidence(artifact, "after", input_hash="d4e5fa678")

            records = summarize.summarize_directory(artifact)

            with self.assertRaisesRegex(
                ValueError, "before/after fixture_segment_recall input hashes differ"
            ):
                summarize.validate_records(records, artifact)

    def test_validation_rejects_fixture_rows_or_queries_different_from_selected_values(self):
        for selected_rows, selected_queries, sidecar_key, selected_key in (
            ("100000", "16", "segment_rows", "fixture_rows"),
            ("256", "200", "segment_queries", "fixture_queries"),
        ):
            with self.subTest(rows=selected_rows, queries=selected_queries), tempfile.TemporaryDirectory() as root:
                artifact = Path(root)
                self._write_complete_configured_matrix(artifact, "requested")
                for label in ("before", "after"):
                    config = artifact / label / "config.env"
                    config.write_text(
                        config.read_text(encoding="utf-8").replace(
                            "fixture_rows=256\nfixture_queries=16\n",
                            f"fixture_rows={selected_rows}\nfixture_queries={selected_queries}\n",
                            1,
                        ),
                        encoding="utf-8",
                    )
                    self._write_fixture_evidence(artifact, label)

                with self.assertRaisesRegex(
                    ValueError,
                    rf"before: fixture sidecar {sidecar_key} does not match selected {selected_key}",
                ):
                    summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_requested_fixture_evidence_when_all_artifacts_are_missing(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")

            with self.assertRaisesRegex(ValueError, "fixture_segment_recall"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_requested_fixture_evidence_when_an_artifact_is_missing(self):
        for filename in (
            "fixture_segment_recall.env",
            "fixture_segment_recall.log",
            "fixture_segment_recall.time",
            "fixture_segment_recall.status",
        ):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as root:
                artifact = Path(root)
                self._write_complete_configured_matrix(artifact, "requested")
                for label in ("before", "after"):
                    self._write_fixture_evidence(artifact, label)
                (artifact / "before" / filename).unlink()

                with self.assertRaisesRegex(ValueError, "fixture_segment_recall"):
                    summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_fixture_sidecar_identity_mismatches(self):
        for key, replacement in (
            ("source", "synthetic"),
            ("fixture_worktree_path", "/unexpected/fixture.parquet"),
            ("fixture_input_hash", "differenthash"),
        ):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as root:
                artifact = Path(root)
                self._write_complete_configured_matrix(artifact, "requested")
                for label in ("before", "after"):
                    self._write_fixture_evidence(artifact, label)
                sidecar = artifact / "before" / "fixture_segment_recall.env"
                original = (
                    "fixture"
                    if key == "source"
                    else "/worktrees/before/bench/data/dbpedia-openai-100k.parquet"
                    if key == "fixture_worktree_path"
                    else "f1a2ce123"
                )
                sidecar.write_text(
                    sidecar.read_text(encoding="utf-8").replace(
                        f"{key}={original}\n", f"{key}={replacement}\n", 1
                    ),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(ValueError, key):
                    summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_missing_or_empty_fixture_sidecar_input_hash(self):
        for label in ("before", "after"):
            for replacement in (None, ""):
                with self.subTest(label=label, replacement=replacement), tempfile.TemporaryDirectory() as root:
                    artifact = Path(root)
                    self._write_complete_configured_matrix(artifact, "requested")
                    for fixture_label in ("before", "after"):
                        self._write_fixture_evidence(artifact, fixture_label)
                    sidecar = artifact / label / "fixture_segment_recall.env"
                    sidecar_text = sidecar.read_text(encoding="utf-8")
                    replacement_line = "" if replacement is None else "fixture_input_hash=\n"
                    sidecar.write_text(
                        sidecar_text.replace(
                            "fixture_input_hash=f1a2ce123\n", replacement_line, 1
                        ),
                        encoding="utf-8",
                    )

                    with self.assertRaisesRegex(
                        ValueError, rf"{label}: fixture_input_hash must be non-empty"
                    ):
                        summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_truncated_fixture_filtered_metrics(self):
        with tempfile.TemporaryDirectory() as root:
            artifact = Path(root)
            self._write_complete_configured_matrix(artifact, "requested")
            for label in ("before", "after"):
                self._write_fixture_evidence(artifact, label)
            fixture_log = artifact / "before" / "fixture_segment_recall.log"
            final_filtered_row = "  64  1.0000 / 1.0000     10.0 / 11.0     100 / 90"
            before, separator, after = fixture_log.read_text(encoding="utf-8").rpartition(final_filtered_row)
            fixture_log.write_text(
                before + after if separator else fixture_log.read_text(encoding="utf-8"),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "segment_k64_filtered"):
                summarize.validate_records(summarize.summarize_directory(artifact), artifact)

    def test_validation_rejects_conflicting_duplicate_fixture_metadata(self):
        conflicts = {
            "source": "loaded 256 rows from fixture /unexpected/fixture.parquet; input hash=f1a2ce123",
            "path": "fixture_worktree_path=/unexpected/fixture.parquet",
            "shape": "==== recall vs segment count — 256 rows x 999-dim, k=10, ef_search=32 ====",
            "hash": "loaded 256 rows from fixture /worktrees/before/bench/data/dbpedia-openai-100k.parquet; input hash=differenthash",
        }
        for kind, duplicate in conflicts.items():
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as root:
                artifact = Path(root)
                self._write_complete_configured_matrix(artifact, "requested")
                for label in ("before", "after"):
                    self._write_fixture_evidence(artifact, label)
                fixture_log = artifact / "before" / "fixture_segment_recall.log"
                fixture_log.write_text(
                    fixture_log.read_text(encoding="utf-8") + f"\n{duplicate}\n",
                    encoding="utf-8",
                )

                with self.assertRaises(ValueError):
                    summarize.validate_records(summarize.summarize_directory(artifact), artifact)


if __name__ == "__main__":
    unittest.main()
