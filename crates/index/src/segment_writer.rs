//! Pure, in-memory serialization of a built HNSW graph into the on-disk
//! segment format ([`crate::segment_format`]). **Zero file I/O and zero
//! `strata-storage` dependency by design** — the crate-ownership decision
//! recorded in `docs/superpowers/specs/2026-07-25-s1-w3-design-amendment.md`
//! §4: `crates/index` produces the bytes, `crates/txn` writes and fsyncs
//! them through `strata_storage::write_bytes` (which carries the chaos
//! checkpoint).
//!
//! The source graph is addressed by **segment-local ordinals `0..N`**, not
//! global row-ids — see
//! `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §3b for
//! why (`row_ids` becomes a direct positional dump that is ascending by
//! construction, and the working index's `NodeTable` stays inside its first
//! 65536-id chunk regardless of the commit's actual row-id range).

use crate::hnsw::IndexError;
use crate::node_source::NodeSource;
use crate::segment_format::{
    FLAG_LITTLE_ENDIAN, HEADER_LEN, MAGIC, METRIC_L2, OFF_BODY_CRC, OFF_DIM, OFF_EF_CONSTRUCTION,
    OFF_ENTRY_POINT, OFF_FLAGS, OFF_FORMAT_VERSION, OFF_HEADER_CRC, OFF_M, OFF_M_L, OFF_MAGIC,
    OFF_MAX_LEVEL, OFF_METRIC, OFF_MMAX, OFF_MMAX0, OFF_NODE_COUNT, OFF_ROW_ID_MAX, OFF_ROW_ID_MIN,
    OFF_SECTION_LEN, OFF_SECTION_OFF, SECTION_COUNT, SEGMENT_FORMAT_VERSION, SegmentParams,
    VECTORS_ALIGN, align_up,
};

#[cfg(target_endian = "big")]
compile_error!("the Strata segment format requires a little-endian target");

fn too_large(what: &str) -> IndexError {
    IndexError::SegmentTooLarge(what.to_string())
}

/// Serializes `source`'s graph — addressed by local ordinals
/// `0..row_ids.len()` — into a complete, CRC-checked segment image.
///
/// `row_ids[local]` is the global row-id that local ordinal `local`
/// represents. It must be strictly ascending, which it is by construction
/// for a segment built from one transaction's contiguous row-id claim with
/// null-vector rows skipped.
///
/// # Errors
///
/// - [`IndexError::SegmentEmpty`] if `row_ids` is empty, if `source` has no
///   established dimension, or if `source` has no entry point. A commit
///   with no vectors must not call this at all — it writes no segment (see
///   the W3.2 amendment §3c).
/// - [`IndexError::SegmentCorrupt`] if `source` is not a well-formed
///   `0..N`-keyed graph: a missing node or vector at some ordinal, a level
///   above 255, an entry point outside `0..N`, or a neighbor ordinal
///   outside `0..N`.
/// - [`IndexError::DimensionMismatch`] if any node's vector length differs
///   from `source.dimension()`.
/// - [`IndexError::SegmentTooLarge`] if any count or section length
///   overflows the format's `u32` fields.
#[allow(clippy::too_many_lines)] // One linear, top-to-bottom encoder; splitting it would
// scatter the format's field order across functions, which is exactly the
// thing that must stay readable in one screenful next to the format table.
pub(crate) fn encode_segment<S: NodeSource>(
    source: &S,
    row_ids: &[u64],
    params: SegmentParams,
) -> Result<Box<[u8]>, IndexError> {
    if row_ids.is_empty() {
        return Err(IndexError::SegmentEmpty);
    }
    let n = row_ids.len();
    let node_count = u32::try_from(n).map_err(|_| too_large("node_count exceeds u32"))?;
    if row_ids.windows(2).any(|w| w[0] >= w[1]) {
        return Err(IndexError::SegmentCorrupt(
            "row_ids must be strictly ascending".to_string(),
        ));
    }

    let dim = source.dimension();
    if dim == 0 {
        return Err(IndexError::SegmentEmpty);
    }
    let dim_u32 = u32::try_from(dim).map_err(|_| too_large("dim exceeds u32"))?;

    // Levels, plus the per-node well-formedness checks. Done up front so a
    // malformed source fails before any buffer is sized.
    let mut levels: Vec<u8> = Vec::with_capacity(n);
    for local in 0..u64::from(node_count) {
        let level = source.level(local).ok_or_else(|| {
            IndexError::SegmentCorrupt(format!(
                "no node at local ordinal {local}: every ordinal in 0..{node_count} must be populated"
            ))
        })?;
        let level = u8::try_from(level).map_err(|_| {
            IndexError::SegmentCorrupt(format!("node {local}'s level {level} exceeds u8"))
        })?;
        let vector = source.vector(local).ok_or_else(|| {
            IndexError::SegmentCorrupt(format!("no vector at local ordinal {local}"))
        })?;
        if vector.len() != dim {
            return Err(IndexError::DimensionMismatch {
                query_len: vector.len(),
                expected: dim,
            });
        }
        levels.push(level);
    }
    let max_level = usize::from(levels.iter().copied().max().unwrap_or(0));
    let max_level_u32 = u32::try_from(max_level).map_err(|_| too_large("max_level exceeds u32"))?;

    let Some((entry_local, _)) = source.entry_point() else {
        return Err(IndexError::SegmentEmpty);
    };
    let entry_point = u32::try_from(entry_local)
        .ok()
        .filter(|&e| e < node_count)
        .ok_or_else(|| {
            IndexError::SegmentCorrupt(format!(
                "entry point {entry_local} is outside 0..{node_count}"
            ))
        })?;

    // CSR adjacency, one (offsets, neighbors) pair per layer.
    let mut layer_offsets: Vec<Vec<u32>> = Vec::with_capacity(max_level + 1);
    let mut layer_neighbors: Vec<Vec<u32>> = Vec::with_capacity(max_level + 1);
    let mut buf: Vec<u64> = Vec::new();
    for layer in 0..=max_level {
        let mut offsets: Vec<u32> = Vec::with_capacity(n + 1);
        let mut neighbors: Vec<u32> = Vec::new();
        offsets.push(0);
        for local in 0..u64::from(node_count) {
            source.neighbors_into(local, layer, &mut buf);
            for &neighbor in &buf {
                let ordinal = u32::try_from(neighbor)
                    .ok()
                    .filter(|&o| o < node_count)
                    .ok_or_else(|| {
                        IndexError::SegmentCorrupt(format!(
                            "neighbor {neighbor} of node {local} at layer {layer} is outside 0..{node_count}"
                        ))
                    })?;
                neighbors.push(ordinal);
            }
            let end = u32::try_from(neighbors.len())
                .map_err(|_| too_large("a layer's neighbor count exceeds u32"))?;
            offsets.push(end);
        }
        layer_offsets.push(offsets);
        layer_neighbors.push(neighbors);
    }

    // Section sizing and placement. `row_ids` starts at HEADER_LEN, which is
    // already 64-byte (hence 8-byte) aligned, so it needs no padding.
    let row_ids_len = n
        .checked_mul(8)
        .ok_or_else(|| too_large("row_ids section"))?;
    let levels_len = n;
    let adjacency_len = layer_offsets
        .iter()
        .zip(&layer_neighbors)
        .try_fold(0_usize, |acc, (offsets, neighbors)| {
            let per_layer = offsets
                .len()
                .checked_add(neighbors.len())
                .and_then(|elems| elems.checked_mul(4))?;
            acc.checked_add(per_layer)
        })
        .ok_or_else(|| too_large("adjacency section"))?;
    let vectors_len = n
        .checked_mul(dim)
        .and_then(|elems| elems.checked_mul(4))
        .ok_or_else(|| too_large("vectors section"))?;

    let off_row_ids = HEADER_LEN;
    let off_levels = off_row_ids
        .checked_add(row_ids_len)
        .ok_or_else(|| too_large("levels offset"))?;
    let off_adjacency = align_up(
        off_levels
            .checked_add(levels_len)
            .ok_or_else(|| too_large("adjacency offset"))?,
        4,
    );
    let off_vectors = align_up(
        off_adjacency
            .checked_add(adjacency_len)
            .ok_or_else(|| too_large("vectors offset"))?,
        VECTORS_ALIGN,
    );
    let total = off_vectors
        .checked_add(vectors_len)
        .ok_or_else(|| too_large("total file length"))?;

    let mut out = vec![0_u8; total];

    // --- body ---
    write_at(
        &mut out,
        off_row_ids,
        bytemuck::cast_slice::<u64, u8>(row_ids),
    )?;
    write_at(&mut out, off_levels, &levels)?;

    let mut cursor = off_adjacency;
    for (offsets, neighbors) in layer_offsets.iter().zip(&layer_neighbors) {
        let offsets_bytes = bytemuck::cast_slice::<u32, u8>(offsets);
        write_at(&mut out, cursor, offsets_bytes)?;
        cursor += offsets_bytes.len();
        let neighbors_bytes = bytemuck::cast_slice::<u32, u8>(neighbors);
        write_at(&mut out, cursor, neighbors_bytes)?;
        cursor += neighbors_bytes.len();
    }

    let mut cursor = off_vectors;
    for local in 0..u64::from(node_count) {
        let vector = source.vector(local).ok_or_else(|| {
            IndexError::SegmentCorrupt(format!("no vector at local ordinal {local}"))
        })?;
        let vector_bytes = bytemuck::cast_slice::<f32, u8>(vector);
        write_at(&mut out, cursor, vector_bytes)?;
        cursor += vector_bytes.len();
    }

    // --- header ---
    write_at(&mut out, OFF_MAGIC, &MAGIC)?;
    write_at(
        &mut out,
        OFF_FORMAT_VERSION,
        &SEGMENT_FORMAT_VERSION.to_le_bytes(),
    )?;
    write_at(&mut out, OFF_FLAGS, &FLAG_LITTLE_ENDIAN.to_le_bytes())?;
    write_at(&mut out, OFF_NODE_COUNT, &node_count.to_le_bytes())?;
    write_at(&mut out, OFF_DIM, &dim_u32.to_le_bytes())?;
    write_at(&mut out, OFF_MAX_LEVEL, &max_level_u32.to_le_bytes())?;
    write_at(&mut out, OFF_ENTRY_POINT, &entry_point.to_le_bytes())?;
    write_at(&mut out, OFF_METRIC, &[METRIC_L2])?;
    write_at(
        &mut out,
        OFF_M,
        &u16::try_from(params.m)
            .map_err(|_| too_large("m exceeds u16"))?
            .to_le_bytes(),
    )?;
    write_at(
        &mut out,
        OFF_MMAX0,
        &u16::try_from(params.mmax0)
            .map_err(|_| too_large("mmax0 exceeds u16"))?
            .to_le_bytes(),
    )?;
    write_at(
        &mut out,
        OFF_MMAX,
        &u16::try_from(params.mmax)
            .map_err(|_| too_large("mmax exceeds u16"))?
            .to_le_bytes(),
    )?;
    write_at(
        &mut out,
        OFF_EF_CONSTRUCTION,
        &u16::try_from(params.ef_construction)
            .map_err(|_| too_large("ef_construction exceeds u16"))?
            .to_le_bytes(),
    )?;
    write_at(&mut out, OFF_M_L, &params.m_l.to_le_bytes())?;
    // Non-empty and strictly ascending, both checked above.
    let (Some(&row_id_min), Some(&row_id_max)) = (row_ids.first(), row_ids.last()) else {
        return Err(IndexError::SegmentEmpty);
    };
    write_at(&mut out, OFF_ROW_ID_MIN, &row_id_min.to_le_bytes())?;
    write_at(&mut out, OFF_ROW_ID_MAX, &row_id_max.to_le_bytes())?;

    let section_offs = [off_row_ids, off_levels, off_adjacency, off_vectors];
    let section_lens = [row_ids_len, levels_len, adjacency_len, vectors_len];
    for i in 0..SECTION_COUNT {
        let off = u64::try_from(section_offs[i]).map_err(|_| too_large("section offset"))?;
        write_at(&mut out, OFF_SECTION_OFF + i * 8, &off.to_le_bytes())?;
        let len = u32::try_from(section_lens[i]).map_err(|_| {
            IndexError::SegmentTooLarge(format!(
                "section {i} is {} bytes, which exceeds this format's u32 section length",
                section_lens[i]
            ))
        })?;
        write_at(&mut out, OFF_SECTION_LEN + i * 4, &len.to_le_bytes())?;
    }

    // CRCs last: the body CRC covers every byte after the header (padding
    // included, which is why the buffer is zero-initialized), and the header
    // CRC covers the header up to but excluding itself.
    let body_crc = crc32c::crc32c(out.get(HEADER_LEN..).unwrap_or(&[]));
    write_at(&mut out, OFF_BODY_CRC, &body_crc.to_le_bytes())?;
    let header_crc = crc32c::crc32c(out.get(..OFF_HEADER_CRC).unwrap_or(&[]));
    write_at(&mut out, OFF_HEADER_CRC, &header_crc.to_le_bytes())?;

    Ok(out.into_boxed_slice())
}

/// Copies `src` into `out` at `at`, or reports a corrupt/oversized layout
/// rather than panicking on an out-of-range slice index. Every call site
/// computed `at` from this function's own sizing arithmetic, so a failure
/// here is an encoder bug, not bad input — but it must surface as a typed
/// error, not a panic on the commit path.
fn write_at(out: &mut [u8], at: usize, src: &[u8]) -> Result<(), IndexError> {
    let end = at
        .checked_add(src.len())
        .ok_or_else(|| too_large("write offset"))?;
    let src_len = src.len();
    let out_len = out.len();
    let dst = out.get_mut(at..end).ok_or_else(|| {
        IndexError::SegmentCorrupt(format!(
            "encoder tried to write {src_len} bytes at offset {at} of a {out_len}-byte buffer"
        ))
    })?;
    dst.copy_from_slice(src);
    Ok(())
}

#[cfg(all(test, not(loom)))]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    // Section offsets read back out of the header in these tests are
    // encoded from `usize` values in the first place (see `encode_segment`'s
    // `u64::try_from(section_offs[i])`), so the round trip through `u64` and
    // back to `usize` here is exact on any platform this project targets.
    clippy::cast_possible_truncation
)]
mod tests {
    use super::*;
    use crate::hnsw::{EfConstruction, HnswIndex, MaxConnections, MaxElements, MaxLayers};
    use crate::segment_format::{OFF_HEADER_CRC, SECTION_ADJACENCY, SECTION_VECTORS};

    /// A fresh index keyed by segment-local ordinals `0..n`, exactly as
    /// `crates/txn`'s segment builder will key it. Quasi-random,
    /// non-collinear coordinates for the same reason `hnsw.rs`'s own
    /// `insert_cluster` uses them: collinear points let the diversity
    /// heuristic prune the graph into a degenerate shape.
    #[allow(clippy::cast_possible_truncation)]
    fn local_keyed_index(n: usize) -> HnswIndex {
        const PHI: f64 = 0.618_033_988_749_895;
        const SQRT2: f64 = 0.414_213_562_373_095;
        const SQRT3: f64 = 0.732_050_807_568_877;
        let index = HnswIndex::new(
            MaxConnections(4),
            MaxElements(n + 1),
            MaxLayers(16),
            EfConstruction(20),
        )
        .unwrap();
        for local in 0..n as u64 {
            let f = local as f64;
            index
                .insert_owned(
                    local,
                    vec![
                        ((f * PHI).fract() * 1000.0) as f32,
                        ((f * SQRT2).fract() * 1000.0) as f32,
                        ((f * SQRT3).fract() * 1000.0) as f32,
                    ],
                )
                .unwrap();
        }
        index
    }

    fn read_u32(bytes: &[u8], at: usize) -> u32 {
        let mut buf = [0_u8; 4];
        buf.copy_from_slice(&bytes[at..at + 4]);
        u32::from_le_bytes(buf)
    }

    fn read_u64(bytes: &[u8], at: usize) -> u64 {
        let mut buf = [0_u8; 8];
        buf.copy_from_slice(&bytes[at..at + 8]);
        u64::from_le_bytes(buf)
    }

    #[test]
    fn header_records_the_magic_version_and_geometry() {
        let index = local_keyed_index(20);
        let row_ids: Vec<u64> = (100..120).collect();
        let bytes = index.to_segment_bytes(&row_ids).unwrap();

        assert_eq!(&bytes[0..8], &MAGIC);
        assert_eq!(read_u32(&bytes, OFF_FORMAT_VERSION), SEGMENT_FORMAT_VERSION);
        assert_eq!(read_u32(&bytes, OFF_FLAGS), FLAG_LITTLE_ENDIAN);
        assert_eq!(read_u32(&bytes, OFF_NODE_COUNT), 20);
        assert_eq!(read_u32(&bytes, OFF_DIM), 3);
        assert_eq!(bytes[OFF_METRIC], METRIC_L2);
        assert_eq!(read_u64(&bytes, OFF_ROW_ID_MIN), 100);
        assert_eq!(read_u64(&bytes, OFF_ROW_ID_MAX), 119);
        assert!(
            read_u32(&bytes, OFF_ENTRY_POINT) < 20,
            "the entry point must be a local ordinal, not a row-id"
        );
    }

    #[test]
    fn sections_are_aligned_contiguous_and_cover_the_whole_file() {
        let index = local_keyed_index(20);
        let row_ids: Vec<u64> = (0..20).collect();
        let bytes = index.to_segment_bytes(&row_ids).unwrap();

        let offs: Vec<usize> = (0..SECTION_COUNT)
            .map(|i| read_u64(&bytes, OFF_SECTION_OFF + i * 8) as usize)
            .collect();
        let lens: Vec<usize> = (0..SECTION_COUNT)
            .map(|i| read_u32(&bytes, OFF_SECTION_LEN + i * 4) as usize)
            .collect();

        assert_eq!(offs[0], HEADER_LEN, "row_ids starts right after the header");
        assert_eq!(lens[0], 20 * 8);
        assert_eq!(lens[1], 20);
        assert_eq!(lens[SECTION_VECTORS], 20 * 3 * 4);
        assert_eq!(
            offs[SECTION_VECTORS] % VECTORS_ALIGN,
            0,
            "the vectors section start must be 64-byte aligned"
        );
        assert_eq!(offs[SECTION_ADJACENCY] % 4, 0);
        for i in 1..SECTION_COUNT {
            assert!(
                offs[i] >= offs[i - 1] + lens[i - 1],
                "section {i} must not overlap section {}",
                i - 1
            );
        }
        assert_eq!(
            bytes.len(),
            offs[SECTION_VECTORS] + lens[SECTION_VECTORS],
            "the file must end exactly where the last section does"
        );
    }

    #[test]
    fn both_crcs_match_the_bytes_they_cover() {
        let index = local_keyed_index(10);
        let row_ids: Vec<u64> = (0..10).collect();
        let bytes = index.to_segment_bytes(&row_ids).unwrap();

        assert_eq!(
            read_u32(&bytes, OFF_BODY_CRC),
            crc32c::crc32c(&bytes[HEADER_LEN..])
        );
        assert_eq!(
            read_u32(&bytes, OFF_HEADER_CRC),
            crc32c::crc32c(&bytes[..OFF_HEADER_CRC])
        );
    }

    #[test]
    fn row_ids_are_a_direct_positional_dump_of_the_caller_supplied_array() {
        // The W3.2 amendment section 3b property: keying the working index
        // 0..N makes the row_ids section a straight copy, with no remap
        // pass and no reordering.
        let index = local_keyed_index(5);
        let row_ids: Vec<u64> = vec![7, 11, 13, 5_000_000, 5_000_001];
        let bytes = index.to_segment_bytes(&row_ids).unwrap();
        let off = read_u64(&bytes, OFF_SECTION_OFF) as usize;
        let decoded: &[u64] = bytemuck::cast_slice(&bytes[off..off + 5 * 8]);
        assert_eq!(decoded, row_ids.as_slice());
    }

    #[test]
    fn an_empty_row_id_list_is_rejected_rather_than_producing_an_empty_segment() {
        // A vector-less commit must write no segment at all (W3.2
        // amendment section 3c), so this path must never be reachable with
        // an empty list -- and must fail loudly if it ever is.
        let index = local_keyed_index(3);
        assert!(matches!(
            index.to_segment_bytes(&[]),
            Err(IndexError::SegmentEmpty)
        ));
    }

    #[test]
    fn non_ascending_row_ids_are_rejected() {
        let index = local_keyed_index(3);
        assert!(matches!(
            index.to_segment_bytes(&[5, 5, 9]),
            Err(IndexError::SegmentCorrupt(_))
        ));
        assert!(matches!(
            index.to_segment_bytes(&[9, 5, 1]),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }

    #[test]
    fn a_row_id_list_longer_than_the_graph_is_rejected_rather_than_encoding_garbage() {
        let index = local_keyed_index(3);
        let row_ids: Vec<u64> = (0..5).collect();
        assert!(matches!(
            index.to_segment_bytes(&row_ids),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }
}
