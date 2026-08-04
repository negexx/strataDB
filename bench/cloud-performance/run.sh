#!/usr/bin/env bash
set -euo pipefail

before_ref="${1:?before revision is required}"
after_ref="${2:?after revision is required}"
artifact_dir="${3:-${RUNNER_TEMP:-/tmp}/strata-cloud-performance-artifacts}"

repo_root="$(git rev-parse --show-toplevel)"
mkdir -p "$artifact_dir"
artifact_dir="$(cd "$artifact_dir" && pwd)"
before_sha="$(git rev-parse --verify "${before_ref}^{commit}")"
after_sha="$(git rev-parse --verify "${after_ref}^{commit}")"
target_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/strata-cloud-performance-targets"
mkdir -p "$target_root"
bench_seed="${STRATA_BENCH_SEED:-20260801}"
growth_warmups="${STRATA_GROWTH_WARMUP_RUNS:-1}"
growth_repetitions="${STRATA_GROWTH_REPETITIONS:-5}"
segment_rows="${STRATA_SEG_ROWS:-256}"
segment_queries="${STRATA_SEG_QUERIES:-16}"
segment_warmups="${STRATA_SEG_WARMUP_RUNS:-1}"
segment_repetitions="${STRATA_SEG_REPETITIONS:-5}"
lifecycle_rows="${STRATA_LIFECYCLE_ROWS:-64}"
lifecycle_batch_rows="${STRATA_LIFECYCLE_BATCH_ROWS:-1}"
lifecycle_pins="${STRATA_PINNED_SNAPSHOTS:-0,1,4,16,64}"
lifecycle_warmups="${STRATA_LIFECYCLE_WARMUP_RUNS:-1}"
lifecycle_repetitions="${STRATA_LIFECYCLE_REPETITIONS:-5}"
fixture_repo="Qdrant/dbpedia-entities-openai3-text-embedding-3-small-512-100K"
fixture_revision="56e6849a3d0f7913e56b475bf92c0064c93b576d"
fixture_file="data/train-00000-of-00001.parquet"
fixture_size_bytes=363758493
fixture_sha256="5ea400d91cba9b27fa55fc659e48f7bda8cba68443f087a15ddbc0e42acd049d"
fixture_path="${STRATA_BENCH_FIXTURE:-$repo_root/bench/data/dbpedia-openai-100k.parquet}"
fixture_evidence="not-requested"
if [[ "${STRATA_REAL_FIXTURE:-0}" == "1" ]]; then
  fixture_evidence="requested"
fi
segment_dimension=512
segment_k=10
segment_ef_search=32
segment_m=16
segment_ef_construction=100
segment_max_layer=16
worktree_root="$(mktemp -d "${RUNNER_TEMP:-/tmp}/strata-cloud-performance-worktrees.XXXXXX")"
before_dir="$worktree_root/before"
after_dir="$worktree_root/after"

cleanup() {
  git -C "$repo_root" worktree remove --force "$before_dir" >/dev/null 2>&1 || true
  git -C "$repo_root" worktree remove --force "$after_dir" >/dev/null 2>&1 || true
  rmdir "$worktree_root" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$before_dir" "$before_sha"
git -C "$repo_root" worktree add --detach "$after_dir" "$after_sha"

{
  printf 'before_ref=%s\n' "$before_ref"
  printf 'before_sha=%s\n' "$before_sha"
  printf 'after_ref=%s\n' "$after_ref"
  printf 'after_sha=%s\n' "$after_sha"
  printf 'runner.os=%s\nrunner.arch=%s\n' "${RUNNER_OS:-unknown}" "${RUNNER_ARCH:-unknown}"
  uname -a || true
  rustc -Vv
  cargo -Vv
  sha256sum Cargo.lock
  printf 'filesystem_path=%s\n' "$repo_root"
  df -T "$repo_root" || df -k "$repo_root"
  if command -v lscpu >/dev/null 2>&1; then lscpu; fi
  if [[ -r /proc/meminfo ]]; then grep -E 'MemTotal|MemAvailable' /proc/meminfo; fi
  printf 'cache_policy=separate target directory per revision; warmup excluded by benchmark settings; OS caches not forcibly flushed\n'
  printf 'fixture_repo=%s\nfixture_revision=%s\nfixture_file=%s\nfixture_size_bytes=%s\nfixture_sha256=%s\n' \
    "$fixture_repo" "$fixture_revision" "$fixture_file" "$fixture_size_bytes" "$fixture_sha256"
} | tee "$artifact_dir/provenance.log"

write_config() {
  local label="$1"
  local sha="$2"
  local revision_dir="$3"
  local lock_sha
  mkdir -p "$artifact_dir/$label"
  lock_sha="$(sha256sum "$revision_dir/Cargo.lock" | awk '{print $1}')"
  cat > "$artifact_dir/$label/config.env" <<EOF
label=$label
revision=$sha
lockfile_sha256=$lock_sha
seed=$bench_seed
source=synthetic
fixture_evidence=$fixture_evidence
fixture_repo=$fixture_repo
fixture_revision=$fixture_revision
fixture_file=$fixture_file
fixture_size_bytes=$fixture_size_bytes
fixture_sha256=$fixture_sha256
workload_signature=synthetic-seed-$bench_seed-dim512-hnsw-M16-efc100-efsearch32-k10
manifest_points=1,10,20,40,80,160
manifest_warmup_runs=$growth_warmups
manifest_repetitions=$growth_repetitions
segment_rows=$segment_rows
segment_queries=$segment_queries
segment_dimension=$segment_dimension
segment_k=$segment_k
segment_ef_search=$segment_ef_search
segment_m=$segment_m
segment_ef_construction=$segment_ef_construction
segment_max_layer=$segment_max_layer
segment_points=1,2,4,8,16,32,64
segment_warmup_runs=$segment_warmups
segment_repetitions=$segment_repetitions
lifecycle_rows=$lifecycle_rows
lifecycle_batch_rows=$lifecycle_batch_rows
lifecycle_pins=$lifecycle_pins
lifecycle_warmup_runs=$lifecycle_warmups
lifecycle_repetitions=$lifecycle_repetitions
command_manifest=CARGO_TARGET_DIR=<revision-target> STRATA_GROWTH_COMMITS=<point> STRATA_GROWTH_WARMUP_RUNS=$growth_warmups STRATA_GROWTH_REPETITIONS=$growth_repetitions cargo bench -p strata-bench --bench manifest_growth_bench -- --noplot
command_segment=CARGO_TARGET_DIR=<revision-target> STRATA_BENCH_SOURCE=synthetic STRATA_BENCH_SEED=$bench_seed STRATA_SEG_ROWS=$segment_rows STRATA_SEG_QUERIES=$segment_queries STRATA_SEG_WARMUP_RUNS=$segment_warmups STRATA_SEG_REPETITIONS=$segment_repetitions cargo bench -p strata-bench --bench segment_recall_bench -- --noplot
command_lifecycle=CARGO_TARGET_DIR=<revision-target> STRATA_BENCH_SOURCE=synthetic STRATA_BENCH_SEED=$bench_seed STRATA_LIFECYCLE_ROWS=$lifecycle_rows STRATA_LIFECYCLE_BATCH_ROWS=$lifecycle_batch_rows STRATA_PINNED_SNAPSHOTS=<pin-count> STRATA_LIFECYCLE_WARMUP_RUNS=$lifecycle_warmups STRATA_LIFECYCLE_REPETITIONS=$lifecycle_repetitions STRATA_LIFECYCLE_MEASUREMENT=cloud cargo bench -p strata-bench --bench lifecycle_bench -- --noplot
command_fixture_smoke=CARGO_TARGET_DIR=<revision-target> STRATA_BENCH_SOURCE=fixture STRATA_BENCH_FIXTURE=<revision-worktree>/bench/data/dbpedia-openai-100k.parquet STRATA_SEG_ROWS=$segment_rows STRATA_SEG_QUERIES=$segment_queries STRATA_SEG_WARMUP_RUNS=$segment_warmups STRATA_SEG_REPETITIONS=$segment_repetitions cargo bench -p strata-bench --bench segment_recall_bench -- --noplot
EOF
}

record_fixture() {
  local label="$1"
  local sha="$2"
  local revision_dir="$3"
  local destination="$revision_dir/bench/data/dbpedia-openai-100k.parquet"
  local actual_sha
  local actual_size
  if [[ "${STRATA_REAL_FIXTURE:-0}" != "1" ]]; then
    printf 'fixture_status=not-requested\n' > "$artifact_dir/$label/fixture_segment_recall.status"
    return
  fi
  [[ -f "$fixture_path" ]] || { printf 'fixture path missing: %s\n' "$fixture_path" >&2; return 1; }
  actual_size="$(wc -c < "$fixture_path" | tr -d '[:space:]')"
  actual_sha="$(sha256sum "$fixture_path" | awk '{print $1}')"
  [[ "$actual_size" == "$fixture_size_bytes" ]] || { printf 'fixture size mismatch: %s\n' "$actual_size" >&2; return 1; }
  [[ "$actual_sha" == "$fixture_sha256" ]] || { printf 'fixture sha256 mismatch: %s\n' "$actual_sha" >&2; return 1; }
  mkdir -p "$(dirname "$destination")"
  cp "$fixture_path" "$destination"
}

record_fixture_evidence() {
  local label="$1"
  local sha="$2"
  local revision_dir="$3"
  local destination="$revision_dir/bench/data/dbpedia-openai-100k.parquet"
  local log="$artifact_dir/$label/fixture_segment_recall.log"
  local rows
  local source
  local input_hash
  local -a loadings
  mapfile -t loadings < <(
    sed -nE 's/^loaded ([0-9]+) rows from (.*); input hash=([0-9a-f]+)$/\1\t\2\t\3/p' "$log"
  )
  [[ "${#loadings[@]}" == "1" ]] || {
    printf 'fixture benchmark emitted %s input identity lines\n' "${#loadings[@]}" >&2
    return 1
  }
  IFS=$'\t' read -r rows source input_hash <<< "${loadings[0]}"
  [[ "$source" == "fixture $destination" ]] || {
    printf 'fixture source mismatch: %s\n' "$source" >&2
    return 1
  }
  [[ "$rows" == "$segment_rows" ]] || {
    printf 'fixture row count mismatch: %s\n' "$rows" >&2
    return 1
  }
  cat > "$artifact_dir/$label/fixture_segment_recall.env" <<EOF
label=$label
revision=$sha
lockfile_sha256=$(sha256sum "$revision_dir/Cargo.lock" | awk '{print $1}')
source=fixture
fixture_repo=$fixture_repo
fixture_revision=$fixture_revision
fixture_file=$fixture_file
fixture_size_bytes=$fixture_size_bytes
fixture_sha256=$fixture_sha256
fixture_worktree_path=$destination
fixture_source=$source
fixture_input_hash=$input_hash
segment_rows=$segment_rows
segment_queries=$segment_queries
segment_dimension=$segment_dimension
segment_k=$segment_k
segment_ef_search=$segment_ef_search
segment_m=$segment_m
segment_ef_construction=$segment_ef_construction
segment_max_layer=$segment_max_layer
segment_points=1,2,4,8,16,32,64
segment_warmup_runs=$segment_warmups
segment_repetitions=$segment_repetitions
command=CARGO_TARGET_DIR=<revision-target> STRATA_BENCH_SOURCE=fixture STRATA_BENCH_FIXTURE=$destination STRATA_SEG_ROWS=$segment_rows STRATA_SEG_QUERIES=$segment_queries STRATA_SEG_WARMUP_RUNS=$segment_warmups STRATA_SEG_REPETITIONS=$segment_repetitions cargo bench -p strata-bench --bench segment_recall_bench -- --noplot
EOF
  printf 'fixture_status=complete\n' > "$artifact_dir/$label/fixture_segment_recall.status"
}

run_benchmark() {
  local label="$1"
  local sha="$2"
  local revision_dir="$3"
  local benchmark="$4"
  local log="$artifact_dir/$label/${benchmark}.log"
  local timing="$artifact_dir/$label/${benchmark}.time"
  local target_dir="$target_root/$label"
  mkdir -p "$artifact_dir/$label"

  pushd "$revision_dir" >/dev/null
  {
    printf 'label=%s\nrevision=' "$label"
    git rev-parse HEAD
    printf 'lockfile_sha256='
    sha256sum Cargo.lock | awk '{print $1}'
    printf 'benchmark=%s\n' "$benchmark"
    if [[ "$benchmark" == "fixture_segment_recall" ]]; then
      printf 'fixture_repo=%s\nfixture_revision=%s\nfixture_file=%s\nfixture_size_bytes=%s\nfixture_sha256=%s\nfixture_worktree_path=%s\n' \
        "$fixture_repo" "$fixture_revision" "$fixture_file" "$fixture_size_bytes" "$fixture_sha256" \
        "$revision_dir/bench/data/dbpedia-openai-100k.parquet"
    fi
    case "$benchmark" in
      manifest_growth_*)
        commits="${benchmark#manifest_growth_}"
        /usr/bin/time -v -o "$timing" env \
            CARGO_TARGET_DIR="$target_dir" \
            STRATA_GROWTH_COMMITS="$commits" \
            STRATA_GROWTH_WARMUP_RUNS="$growth_warmups" \
            STRATA_GROWTH_REPETITIONS="$growth_repetitions" \
            cargo bench -p strata-bench --bench manifest_growth_bench -- --noplot
        ;;
      segment_recall)
        /usr/bin/time -v -o "$timing" env \
            CARGO_TARGET_DIR="$target_dir" \
            STRATA_BENCH_SOURCE=synthetic \
            STRATA_BENCH_SEED="$bench_seed" \
            STRATA_SEG_ROWS="$segment_rows" \
            STRATA_SEG_QUERIES="$segment_queries" \
            STRATA_SEG_WARMUP_RUNS="$segment_warmups" \
            STRATA_SEG_REPETITIONS="$segment_repetitions" \
            cargo bench -p strata-bench --bench segment_recall_bench -- --noplot
        ;;
      lifecycle)
        /usr/bin/time -v -o "$timing" env \
            CARGO_TARGET_DIR="$target_dir" \
            STRATA_BENCH_SOURCE=synthetic \
            STRATA_BENCH_SEED="$bench_seed" \
            STRATA_LIFECYCLE_ROWS="$lifecycle_rows" \
            STRATA_LIFECYCLE_BATCH_ROWS="$lifecycle_batch_rows" \
            STRATA_PINNED_SNAPSHOTS="$lifecycle_pins" \
            STRATA_LIFECYCLE_WARMUP_RUNS="$lifecycle_warmups" \
            STRATA_LIFECYCLE_REPETITIONS="$lifecycle_repetitions" \
            STRATA_LIFECYCLE_MEASUREMENT=cloud \
            cargo bench -p strata-bench --bench lifecycle_bench -- --noplot
        ;;
      fixture_segment_recall)
        /usr/bin/time -v -o "$timing" env \
            CARGO_TARGET_DIR="$target_dir" \
            STRATA_BENCH_SOURCE=fixture \
            STRATA_BENCH_FIXTURE="$revision_dir/bench/data/dbpedia-openai-100k.parquet" \
            STRATA_SEG_ROWS="$segment_rows" \
            STRATA_SEG_QUERIES="$segment_queries" \
            STRATA_SEG_WARMUP_RUNS="$segment_warmups" \
            STRATA_SEG_REPETITIONS="$segment_repetitions" \
            cargo bench -p strata-bench --bench segment_recall_bench -- --noplot
        ;;
      *)
        printf 'unknown benchmark: %s\n' "$benchmark" >&2
        return 2
        ;;
    esac
  } 2>&1 | tee "$log"
  local status="${PIPESTATUS[0]}"
  popd >/dev/null
  return "$status"
}

for label_dir_sha in "before:$before_dir:$before_sha" "after:$after_dir:$after_sha"; do
  label="${label_dir_sha%%:*}"
  remainder="${label_dir_sha#*:}"
  revision_dir="${remainder%%:*}"
  sha="${remainder#*:}"
  write_config "$label" "$sha" "$revision_dir"
  for commits in 1 10 20 40 80 160; do
    run_benchmark "$label" "$sha" "$revision_dir" "manifest_growth_$commits"
  done
  for benchmark in segment_recall lifecycle; do
    run_benchmark "$label" "$sha" "$revision_dir" "$benchmark"
  done
  record_fixture "$label" "$sha" "$revision_dir"
  if [[ "${STRATA_REAL_FIXTURE:-0}" == "1" ]]; then
    run_benchmark "$label" "$sha" "$revision_dir" fixture_segment_recall
    record_fixture_evidence "$label" "$sha" "$revision_dir"
  fi
done
