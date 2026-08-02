//! Read-only view over one immutable on-disk segment ([`crate::segment_format`]),
//! implementing [`NodeSource`] so `search_layer_generic`/`k_nn_search_generic`
//! traverse it with the identical algorithm they use for the live graph.
//!
//! **Loading is `O(bytes)`**: offset/length validation, one CRC pass over
//! the header and one over the body, one ascending check over `row_ids` and
//! one range check over the adjacency entries — **zero distance
//! evaluations, zero graph construction**. That is the entire recovery win
//! this format exists for (base design doc §1).
//!
//! **Every accessor fails closed.** No indexing, no `unwrap`, no panic: an
//! out-of-range local id yields `None` (or `u64::MAX` for
//! [`NodeSource::row_id`], which the trait forces to be infallible). Binding
//! per `docs/superpowers/specs/2026-07-25-s1-w3-2-design-amendment.md` §4 —
//! `search_layer_generic`'s admission gate calls `vector(local).is_some()`
//! on every visited node, so a corrupt adjacency entry naming an
//! out-of-range ordinal must be excluded there, not crash the search path.
//!
//! **Reentrancy:** nothing here borrows `crate::graph`'s `SEARCH_SCRATCH`,
//! directly or transitively — see [`crate::node_source`]'s module doc for
//! why a future optimisation must not change that.

use crate::hnsw::IndexError;
use crate::node_source::NodeSource;
use crate::segment_format::{
    AlignedBytes, FLAG_LITTLE_ENDIAN, HEADER_LEN, MAGIC, METRIC_L2, NO_ENTRY_POINT, OFF_BODY_CRC,
    OFF_DIM, OFF_EF_CONSTRUCTION, OFF_ENTRY_POINT, OFF_FLAGS, OFF_FORMAT_VERSION, OFF_HEADER_CRC,
    OFF_M, OFF_M_L, OFF_MAGIC, OFF_MAX_LEVEL, OFF_METRIC, OFF_MMAX, OFF_MMAX0, OFF_NODE_COUNT,
    OFF_ROW_ID_MAX, OFF_ROW_ID_MIN, OFF_SECTION_LEN, OFF_SECTION_OFF, SECTION_ADJACENCY,
    SECTION_COUNT, SECTION_LEVELS, SECTION_ROW_IDS, SECTION_VECTORS, SEGMENT_FORMAT_VERSION,
    SegmentParams, VECTORS_ALIGN,
};

#[cfg(target_endian = "big")]
compile_error!("the Strata segment format requires a little-endian target");

fn corrupt(detail: impl Into<String>) -> IndexError {
    IndexError::SegmentCorrupt(detail.into())
}

fn read_u32(bytes: &[u8], at: usize) -> u32 {
    let mut buf = [0_u8; 4];
    if let Some(src) = bytes.get(at..at + 4) {
        buf.copy_from_slice(src);
    }
    u32::from_le_bytes(buf)
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    let mut buf = [0_u8; 8];
    if let Some(src) = bytes.get(at..at + 8) {
        buf.copy_from_slice(src);
    }
    u64::from_le_bytes(buf)
}

fn read_f64(bytes: &[u8], at: usize) -> f64 {
    f64::from_bits(read_u64(bytes, at))
}

fn read_u16(bytes: &[u8], at: usize) -> u16 {
    let mut buf = [0_u8; 2];
    if let Some(src) = bytes.get(at..at + 2) {
        buf.copy_from_slice(src);
    }
    u16::from_le_bytes(buf)
}

pub struct SegmentReader {
    /// The whole file, in a 64-byte-aligned owned allocation that never
    /// moves — every typed view below is a checked cast of a sub-range of
    /// it, so it must outlive them all.
    bytes: AlignedBytes,
    node_count: usize,
    dim: usize,
    max_level: usize,
    /// `(local ordinal, that node's own level)`, or `None` for a segment
    /// with no entry point. The level is read from the `levels` section
    /// rather than assumed equal to `max_level` — see this plan's format
    /// decision #2.
    entry: Option<(u32, usize)>,
    row_ids_off: usize,
    levels_off: usize,
    vectors_off: usize,
    /// `layer_off[l]` = `(byte offset of layer l's offsets array, byte
    /// offset of layer l's neighbors array)`. Computed once at load by
    /// walking the adjacency section, so no accessor ever has to.
    layer_off: Vec<(usize, usize)>,
    params: SegmentParams,
    format_version: u32,
    row_id_min: u64,
    row_id_max: u64,
}

impl SegmentReader {
    /// Validates and loads a complete segment image.
    ///
    /// `raw` is copied into a 64-byte-aligned owned buffer, so the caller
    /// keeps ownership of its own bytes and the reader is `'static`.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::SegmentCorrupt`] — with a message naming the
    /// specific check that failed — if `raw` is shorter than the header,
    /// has the wrong magic/format version/endianness flag/metric, fails
    /// either CRC, declares an out-of-range or misaligned section, declares
    /// a `node_count`/`dim` of zero, has non-ascending `row_ids`, has a
    /// node level above `max_level`, has a malformed CSR offsets array, or
    /// names a neighbor ordinal outside `0..node_count`.
    #[allow(clippy::too_many_lines)] // One linear validation pass whose order is
    // load-bearing (magic -> CRC -> geometry -> sections -> contents);
    // splitting it would hide that order across call sites.
    pub fn from_bytes(raw: &[u8]) -> Result<Self, IndexError> {
        if raw.len() < HEADER_LEN {
            return Err(corrupt(format!(
                "file is {} bytes, shorter than the {HEADER_LEN}-byte header",
                raw.len()
            )));
        }
        let bytes = AlignedBytes::from_slice(raw);
        let b = bytes.as_slice();

        if b.get(OFF_MAGIC..OFF_MAGIC + 8) != Some(&MAGIC[..]) {
            return Err(corrupt("bad magic: not a Strata segment file"));
        }
        let stored_header_crc = read_u32(b, OFF_HEADER_CRC);
        let actual_header_crc = crc32c::crc32c(b.get(..OFF_HEADER_CRC).unwrap_or(&[]));
        if stored_header_crc != actual_header_crc {
            return Err(corrupt(format!(
                "header CRC32C mismatch: stored {stored_header_crc:#010x}, computed {actual_header_crc:#010x}"
            )));
        }

        let format_version = read_u32(b, OFF_FORMAT_VERSION);
        if format_version != SEGMENT_FORMAT_VERSION {
            return Err(corrupt(format!(
                "segment format version {format_version}, but this build reads only {SEGMENT_FORMAT_VERSION}"
            )));
        }
        let flags = read_u32(b, OFF_FLAGS);
        if flags & FLAG_LITTLE_ENDIAN == 0 {
            return Err(corrupt("segment is not flagged little-endian"));
        }
        let metric = b.get(OFF_METRIC).copied().unwrap_or(u8::MAX);
        if metric != METRIC_L2 {
            return Err(corrupt(format!(
                "segment uses metric discriminant {metric}, but this build has only L2 ({METRIC_L2})"
            )));
        }

        let node_count = usize::try_from(read_u32(b, OFF_NODE_COUNT))
            .map_err(|_| corrupt("node_count does not fit in usize"))?;
        let dim = usize::try_from(read_u32(b, OFF_DIM))
            .map_err(|_| corrupt("dim does not fit in usize"))?;
        let max_level = usize::try_from(read_u32(b, OFF_MAX_LEVEL))
            .map_err(|_| corrupt("max_level does not fit in usize"))?;
        if node_count == 0 {
            // A vector-less commit writes no segment at all (W3.2 amendment
            // §3c), so a zero-node segment can only be corruption.
            return Err(corrupt("node_count is zero"));
        }
        if dim == 0 {
            return Err(corrupt("dim is zero"));
        }
        if max_level > usize::from(u8::MAX) {
            return Err(corrupt(format!("max_level {max_level} exceeds u8")));
        }

        // Section table: offsets/lengths in range, non-overlapping,
        // correctly aligned, and exactly the sizes the geometry implies.
        let mut offs = [0_usize; SECTION_COUNT];
        let mut lens = [0_usize; SECTION_COUNT];
        for i in 0..SECTION_COUNT {
            offs[i] = usize::try_from(read_u64(b, OFF_SECTION_OFF + i * 8))
                .map_err(|_| corrupt(format!("section {i}'s offset does not fit in usize")))?;
            lens[i] = usize::try_from(read_u32(b, OFF_SECTION_LEN + i * 4))
                .map_err(|_| corrupt(format!("section {i}'s length does not fit in usize")))?;
            let end = offs[i]
                .checked_add(lens[i])
                .ok_or_else(|| corrupt(format!("section {i}'s extent overflows")))?;
            if offs[i] < HEADER_LEN || end > b.len() {
                return Err(corrupt(format!(
                    "section {i} spans {}..{end}, outside the {}-byte file's body",
                    offs[i],
                    b.len()
                )));
            }
        }
        for i in 1..SECTION_COUNT {
            if offs[i] < offs[i - 1] + lens[i - 1] {
                return Err(corrupt(format!("section {i} overlaps section {}", i - 1)));
            }
        }

        let expected_row_ids = node_count
            .checked_mul(8)
            .ok_or_else(|| corrupt("row_ids section size overflows"))?;
        if lens[SECTION_ROW_IDS] != expected_row_ids {
            return Err(corrupt(format!(
                "row_ids section is {} bytes, expected {expected_row_ids} for {node_count} nodes",
                lens[SECTION_ROW_IDS]
            )));
        }
        if lens[SECTION_LEVELS] != node_count {
            return Err(corrupt(format!(
                "levels section is {} bytes, expected {node_count}",
                lens[SECTION_LEVELS]
            )));
        }
        let expected_vectors = node_count
            .checked_mul(dim)
            .and_then(|elems| elems.checked_mul(4))
            .ok_or_else(|| corrupt("vectors section size overflows"))?;
        if lens[SECTION_VECTORS] != expected_vectors {
            return Err(corrupt(format!(
                "vectors section is {} bytes, expected {expected_vectors}",
                lens[SECTION_VECTORS]
            )));
        }
        if offs[SECTION_ROW_IDS] % 8 != 0 {
            return Err(corrupt("row_ids section is not 8-byte aligned"));
        }
        if offs[SECTION_ADJACENCY] % 4 != 0 {
            return Err(corrupt("adjacency section is not 4-byte aligned"));
        }
        if offs[SECTION_VECTORS] % VECTORS_ALIGN != 0 {
            return Err(corrupt("vectors section is not 64-byte aligned"));
        }

        let stored_body_crc = read_u32(b, OFF_BODY_CRC);
        let actual_body_crc = crc32c::crc32c(b.get(HEADER_LEN..).unwrap_or(&[]));
        if stored_body_crc != actual_body_crc {
            return Err(corrupt(format!(
                "body CRC32C mismatch: stored {stored_body_crc:#010x}, computed {actual_body_crc:#010x}"
            )));
        }

        // Contents. `row_ids` strictly ascending is the precondition the
        // format's "no side table, binary-search the resident array"
        // reverse lookup rests on (base design doc §1).
        let row_ids: &[u64] = b
            .get(offs[SECTION_ROW_IDS]..offs[SECTION_ROW_IDS] + lens[SECTION_ROW_IDS])
            .and_then(|s| bytemuck::try_cast_slice(s).ok())
            .ok_or_else(|| corrupt("row_ids section could not be viewed as [u64]"))?;
        if row_ids.windows(2).any(|w| w[0] >= w[1]) {
            return Err(corrupt("row_ids are not strictly ascending"));
        }

        let levels: &[u8] = b
            .get(offs[SECTION_LEVELS]..offs[SECTION_LEVELS] + lens[SECTION_LEVELS])
            .ok_or_else(|| corrupt("levels section is out of range"))?;
        if levels.iter().any(|&l| usize::from(l) > max_level) {
            return Err(corrupt("a node's level exceeds the segment's max_level"));
        }

        // Walk the adjacency section once: record each layer's two array
        // offsets, and check every CSR offsets array and neighbor ordinal.
        let mut layer_off: Vec<(usize, usize)> = Vec::with_capacity(max_level + 1);
        let mut cursor = offs[SECTION_ADJACENCY];
        let adjacency_end = offs[SECTION_ADJACENCY] + lens[SECTION_ADJACENCY];
        for layer in 0..=max_level {
            let offsets_bytes = (node_count + 1)
                .checked_mul(4)
                .ok_or_else(|| corrupt("a layer's offsets array size overflows"))?;
            let offsets_end = cursor
                .checked_add(offsets_bytes)
                .filter(|&e| e <= adjacency_end)
                .ok_or_else(|| {
                    corrupt(format!(
                        "layer {layer}'s offsets array runs past the adjacency section"
                    ))
                })?;
            let offsets: &[u32] = b
                .get(cursor..offsets_end)
                .and_then(|s| bytemuck::try_cast_slice(s).ok())
                .ok_or_else(|| {
                    corrupt(format!(
                        "layer {layer}'s offsets array could not be viewed as [u32]"
                    ))
                })?;
            if offsets.first() != Some(&0) {
                return Err(corrupt(format!(
                    "layer {layer}'s offsets array does not start at 0"
                )));
            }
            if offsets.windows(2).any(|w| w[0] > w[1]) {
                return Err(corrupt(format!(
                    "layer {layer}'s offsets array is not non-decreasing"
                )));
            }
            let neighbor_count = usize::try_from(offsets.last().copied().unwrap_or(0))
                .map_err(|_| corrupt("a layer's neighbor count does not fit in usize"))?;
            let neighbors_bytes = neighbor_count
                .checked_mul(4)
                .ok_or_else(|| corrupt("a layer's neighbors array size overflows"))?;
            let neighbors_end = offsets_end
                .checked_add(neighbors_bytes)
                .filter(|&e| e <= adjacency_end)
                .ok_or_else(|| {
                    corrupt(format!(
                        "layer {layer}'s neighbors array runs past the adjacency section"
                    ))
                })?;
            let neighbors: &[u32] = b
                .get(offsets_end..neighbors_end)
                .and_then(|s| bytemuck::try_cast_slice(s).ok())
                .ok_or_else(|| {
                    corrupt(format!(
                        "layer {layer}'s neighbors array could not be viewed as [u32]"
                    ))
                })?;
            // Checked once here so the hot loop never has to. `vector()`
            // still fails closed independently, per amendment §4.
            if neighbors
                .iter()
                .any(|&n| usize::try_from(n).is_ok_and(|n| n >= node_count))
            {
                return Err(corrupt(format!(
                    "layer {layer} names a neighbor ordinal outside 0..{node_count}"
                )));
            }
            layer_off.push((cursor, offsets_end));
            cursor = neighbors_end;
        }
        if cursor != adjacency_end {
            return Err(corrupt(format!(
                "adjacency section has {} trailing bytes after {} layers",
                adjacency_end - cursor,
                max_level + 1
            )));
        }

        let entry_raw = read_u32(b, OFF_ENTRY_POINT);
        let entry = if entry_raw == NO_ENTRY_POINT {
            None
        } else {
            let idx = usize::try_from(entry_raw)
                .ok()
                .filter(|&i| i < node_count)
                .ok_or_else(|| {
                    corrupt(format!(
                        "entry point {entry_raw} is outside 0..{node_count}"
                    ))
                })?;
            let level = levels
                .get(idx)
                .copied()
                .ok_or_else(|| corrupt("entry point has no level"))?;
            Some((entry_raw, usize::from(level)))
        };

        Ok(Self {
            node_count,
            dim,
            max_level,
            entry,
            row_ids_off: offs[SECTION_ROW_IDS],
            levels_off: offs[SECTION_LEVELS],
            vectors_off: offs[SECTION_VECTORS],
            layer_off,
            params: SegmentParams {
                m: usize::from(read_u16(b, OFF_M)),
                mmax0: usize::from(read_u16(b, OFF_MMAX0)),
                mmax: usize::from(read_u16(b, OFF_MMAX)),
                ef_construction: usize::from(read_u16(b, OFF_EF_CONSTRUCTION)),
                m_l: read_f64(b, OFF_M_L),
            },
            format_version,
            row_id_min: read_u64(b, OFF_ROW_ID_MIN),
            row_id_max: read_u64(b, OFF_ROW_ID_MAX),
            bytes,
        })
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        self.node_count
    }

    /// The vector dimension every node in this segment carries. Inherent
    /// twin of [`NodeSource::dimension`], so callers that hold a
    /// `SegmentReader` directly need not import the trait.
    #[must_use]
    pub fn dimension(&self) -> usize {
        self.dim
    }

    /// The global row-id local ordinal `local` stands for, or `None` if
    /// `local` is out of range. The fallible form; [`NodeSource::row_id`]
    /// is the infallible one the traversal uses.
    #[must_use]
    pub fn row_id_at(&self, local: u64) -> Option<u64> {
        let idx = usize::try_from(local).ok()?;
        self.row_id_slice().get(idx).copied()
    }

    /// Iterates the global row IDs represented by this already-validated
    /// immutable segment. Recovery uses this to ensure every vector has one
    /// corresponding row-file owner and no vector ID is duplicated across
    /// segments.
    #[must_use]
    pub fn row_ids(&self) -> impl ExactSizeIterator<Item = u64> + '_ {
        self.row_id_slice().iter().copied()
    }

    /// `(row_id_min, row_id_max)`, both inclusive, as recorded in the
    /// header. Informational — never a read path (base design doc §3).
    #[must_use]
    pub fn row_id_range(&self) -> (u64, u64) {
        (self.row_id_min, self.row_id_max)
    }

    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// The HNSW parameters this segment was built with. Not consulted
    /// during search (the graph is already built); kept so the CLI and a
    /// future compactor can report/reproduce them.
    // Not consumed by any production path in W3.2a — kept because the
    // header already carries these fields and a reader that could not
    // report them would make the format non-self-describing. Same pattern
    // as `node.rs`'s `row_id()` accessor.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn params(&self) -> SegmentParams {
        self.params
    }

    fn row_id_slice(&self) -> &[u64] {
        self.bytes
            .as_slice()
            .get(self.row_ids_off..self.row_ids_off + self.node_count * 8)
            .and_then(|s| bytemuck::try_cast_slice(s).ok())
            .unwrap_or(&[])
    }

    fn levels(&self) -> &[u8] {
        self.bytes
            .as_slice()
            .get(self.levels_off..self.levels_off + self.node_count)
            .unwrap_or(&[])
    }

    fn layer_offsets(&self, layer: usize) -> &[u32] {
        let Some(&(offsets_off, neighbors_off)) = self.layer_off.get(layer) else {
            return &[];
        };
        self.bytes
            .as_slice()
            .get(offsets_off..neighbors_off)
            .and_then(|s| bytemuck::try_cast_slice(s).ok())
            .unwrap_or(&[])
    }

    fn layer_neighbors(&self, layer: usize) -> &[u32] {
        let Some(&(_, neighbors_off)) = self.layer_off.get(layer) else {
            return &[];
        };
        // A layer's neighbors run from its own array start to the next
        // layer's offsets array (or, for the last layer, to the end of the
        // adjacency section — which is where the vectors section's padding
        // begins, so the next layer's `offsets_off` is unavailable). Both
        // bounds were validated at load; recompute the end from the CSR
        // offsets array so this stays a pure function of validated state.
        let count = self
            .layer_offsets(layer)
            .last()
            .copied()
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0);
        self.bytes
            .as_slice()
            .get(neighbors_off..neighbors_off + count * 4)
            .and_then(|s| bytemuck::try_cast_slice(s).ok())
            .unwrap_or(&[])
    }
}

impl NodeSource for SegmentReader {
    fn entry_point(&self) -> Option<(u64, usize)> {
        self.entry.map(|(local, level)| (u64::from(local), level))
    }

    fn level(&self, local: u64) -> Option<usize> {
        let idx = usize::try_from(local).ok()?;
        self.levels().get(idx).copied().map(usize::from)
    }

    fn neighbors_into(&self, local: u64, level: usize, out: &mut Vec<u64>) {
        out.clear();
        let Ok(idx) = usize::try_from(local) else {
            return;
        };
        if idx >= self.node_count || level > self.max_level {
            return;
        }
        let offsets = self.layer_offsets(level);
        let (Some(&start), Some(&end)) = (offsets.get(idx), offsets.get(idx + 1)) else {
            return;
        };
        let (Ok(start), Ok(end)) = (usize::try_from(start), usize::try_from(end)) else {
            return;
        };
        let neighbors = self.layer_neighbors(level);
        let Some(slice) = neighbors.get(start..end) else {
            return;
        };
        out.extend(slice.iter().map(|&n| u64::from(n)));
    }

    fn vector(&self, local: u64) -> Option<&[f32]> {
        let idx = usize::try_from(local).ok()?;
        if idx >= self.node_count {
            return None;
        }
        let start = self
            .vectors_off
            .checked_add(idx.checked_mul(self.dim * 4)?)?;
        let end = start.checked_add(self.dim * 4)?;
        let bytes = self.bytes.as_slice().get(start..end)?;
        bytemuck::try_cast_slice(bytes).ok()
    }

    fn row_id(&self, local: u64) -> u64 {
        // The trait cannot return an Option here, so an out-of-range lookup
        // yields a sentinel above `crates/txn`'s enforced 1e9 row-id
        // ceiling: the visibility filter rejects it, so a corrupt segment
        // can never smuggle a result through this path. See
        // [`Self::row_id_at`] for the fallible form.
        self.row_id_at(local).unwrap_or(u64::MAX)
    }

    fn dimension(&self) -> usize {
        self.dim
    }

    // `is_deleted` deliberately uses the trait's `false` default: a segment
    // has no per-node deleted flag. Deletion in the segmented design is the
    // manifest's versioned tombstone set, applied through the caller's
    // `filter` (base design doc §2/§5).
}

#[cfg(all(test, not(loom)))]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_precision_loss,
    // `local` here is always < `n`, and every `n` in this file's fixtures is
    // a small literal (well under u32::MAX), so this never truncates; kept
    // as `as usize` rather than `usize::try_from(..).unwrap()` to match
    // `segment_writer.rs`'s own test module, which allows the same lint for
    // the same reason.
    clippy::cast_possible_truncation
)]
mod tests {
    use crate::hnsw::{
        EfConstruction, HnswIndex, IndexError, MaxConnections, MaxElements, MaxLayers,
    };
    use crate::node_source::NodeSource;
    use crate::segment_reader::SegmentReader;

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

    fn round_trip(n: usize, row_ids: &[u64]) -> (HnswIndex, SegmentReader) {
        let index = local_keyed_index(n);
        let bytes = index.to_segment_bytes(row_ids).unwrap();
        let reader = SegmentReader::from_bytes(&bytes).unwrap();
        (index, reader)
    }

    #[test]
    fn every_nodes_level_vector_and_neighbor_list_survives_the_round_trip() {
        // The single most important property of the whole format: the
        // reader must present byte-identical graph structure to what the
        // in-memory graph exposed, so `search_layer_generic` traverses the
        // same graph either way.
        let n = 40;
        let row_ids: Vec<u64> = (1000..1000 + n as u64).collect();
        let (index, reader) = round_trip(n, &row_ids);
        let source = &index.graph;

        assert_eq!(reader.node_count(), n);
        assert_eq!(reader.dimension(), source.dimension());

        let mut from_graph: Vec<u64> = Vec::new();
        let mut from_segment: Vec<u64> = Vec::new();
        for local in 0..n as u64 {
            assert_eq!(reader.level(local), source.level(local), "level of {local}");
            assert_eq!(
                reader.vector(local),
                source.vector(local),
                "vector of {local}"
            );
            assert_eq!(reader.row_id(local), row_ids[local as usize]);
            let level = source.level(local).unwrap();
            for layer in 0..=level {
                source.neighbors_into(local, layer, &mut from_graph);
                reader.neighbors_into(local, layer, &mut from_segment);
                // The CSR encoding preserves the live graph's slot order,
                // so this is an exact list comparison, not a set one.
                assert_eq!(
                    from_segment, from_graph,
                    "neighbors of node {local} at layer {layer}"
                );
            }
        }
    }

    #[test]
    fn entry_point_round_trips_as_a_local_ordinal_with_its_own_level() {
        let n = 30;
        let row_ids: Vec<u64> = (0..n as u64).collect();
        let (index, reader) = round_trip(n, &row_ids);
        assert_eq!(reader.entry_point(), index.graph.entry_point());
    }

    #[test]
    fn searching_the_segment_returns_the_same_results_as_searching_the_source_graph() {
        // Proves the format is not merely structurally equal but
        // behaviourally equal through the real traversal code -- which is
        // what `SegmentSet::search` will run.
        let n = 200;
        let row_ids: Vec<u64> = (0..n as u64).collect();
        let (index, reader) = round_trip(n, &row_ids);
        let query = [500.0_f32, 500.0, 500.0];

        let from_graph = crate::graph::k_nn_search_generic(
            &index.graph,
            &crate::distance::L2,
            &query,
            10,
            32,
            |_| true,
        )
        .unwrap();
        let from_segment = crate::graph::k_nn_search_generic(
            &reader,
            &crate::distance::L2,
            &query,
            10,
            32,
            |_| true,
        )
        .unwrap();

        assert_eq!(
            from_segment.len(),
            from_graph.len(),
            "same result count: {from_segment:?} vs {from_graph:?}"
        );
        for (a, b) in from_segment.iter().zip(&from_graph) {
            assert_eq!(a.0, b.0, "same local ordinal, in the same rank order");
            assert!(
                (a.1 - b.1).abs() < f32::EPSILON,
                "same distance: {} vs {}",
                a.1,
                b.1
            );
        }
    }

    #[test]
    fn vector_returns_none_for_an_out_of_range_local_id_rather_than_panicking() {
        // Binding requirement, W3.2 amendment section 4: this is the
        // admission gate `search_layer_generic` relies on to fail closed on
        // a corrupt adjacency entry.
        let row_ids: Vec<u64> = (0..10).collect();
        let (_index, reader) = round_trip(10, &row_ids);
        assert!(reader.vector(10).is_none());
        assert!(reader.vector(u64::MAX).is_none());
        assert!(reader.level(10).is_none());
        assert!(reader.level(u64::MAX).is_none());
    }

    #[test]
    fn neighbors_into_clears_its_buffer_and_yields_nothing_for_an_out_of_range_node_or_layer() {
        let row_ids: Vec<u64> = (0..10).collect();
        let (_index, reader) = round_trip(10, &row_ids);
        let mut out = vec![999_u64; 5];
        reader.neighbors_into(10, 0, &mut out);
        assert!(out.is_empty(), "stale contents must never leak through");
        reader.neighbors_into(0, 1_000, &mut out);
        assert!(out.is_empty());
        reader.neighbors_into(u64::MAX, 0, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn row_id_of_an_out_of_range_local_id_is_a_sentinel_no_real_row_can_hold() {
        // `NodeSource::row_id` cannot return an Option (the trait is
        // infallible there), so an out-of-range lookup returns u64::MAX --
        // above `crates/txn`'s enforced 1e9 row-id ceiling, so the
        // visibility filter rejects it and the result is never admitted.
        let row_ids: Vec<u64> = (0..10).collect();
        let (_index, reader) = round_trip(10, &row_ids);
        assert_eq!(reader.row_id(10), u64::MAX);
        assert!(reader.row_id_at(10).is_none());
    }

    #[test]
    fn a_truncated_file_is_rejected_rather_than_read_past_its_end() {
        let row_ids: Vec<u64> = (0..20).collect();
        let index = local_keyed_index(20);
        let bytes = index.to_segment_bytes(&row_ids).unwrap();

        assert!(matches!(
            SegmentReader::from_bytes(&bytes[..bytes.len() / 2]),
            Err(IndexError::SegmentCorrupt(_))
        ));
        assert!(matches!(
            SegmentReader::from_bytes(&bytes[..10]),
            Err(IndexError::SegmentCorrupt(_))
        ));
        assert!(matches!(
            SegmentReader::from_bytes(&[]),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }

    #[test]
    fn a_flipped_body_byte_is_caught_by_the_body_crc() {
        let row_ids: Vec<u64> = (0..20).collect();
        let index = local_keyed_index(20);
        let mut bytes = index.to_segment_bytes(&row_ids).unwrap().into_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(matches!(
            SegmentReader::from_bytes(&bytes),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }

    #[test]
    fn a_flipped_header_byte_is_caught_by_the_header_crc() {
        let row_ids: Vec<u64> = (0..20).collect();
        let index = local_keyed_index(20);
        let mut bytes = index.to_segment_bytes(&row_ids).unwrap().into_vec();
        // Corrupt `node_count` -- a field whose corruption would otherwise
        // be read as a plausible geometry.
        bytes[crate::segment_format::OFF_NODE_COUNT] ^= 0x0F;
        assert!(matches!(
            SegmentReader::from_bytes(&bytes),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }

    #[test]
    fn a_wrong_magic_or_format_version_is_rejected_before_anything_else_is_read() {
        let row_ids: Vec<u64> = (0..5).collect();
        let index = local_keyed_index(5);
        let good = index.to_segment_bytes(&row_ids).unwrap();

        let mut wrong_magic = good.clone().into_vec();
        wrong_magic[0] = b'X';
        assert!(matches!(
            SegmentReader::from_bytes(&wrong_magic),
            Err(IndexError::SegmentCorrupt(_))
        ));

        let mut wrong_version = good.into_vec();
        let bumped = (crate::segment_format::SEGMENT_FORMAT_VERSION + 1).to_le_bytes();
        wrong_version[crate::segment_format::OFF_FORMAT_VERSION
            ..crate::segment_format::OFF_FORMAT_VERSION + 4]
            .copy_from_slice(&bumped);
        // Recompute the header CRC so the version check, not the CRC check,
        // is what rejects it.
        let crc = crc32c::crc32c(&wrong_version[..crate::segment_format::OFF_HEADER_CRC]);
        wrong_version
            [crate::segment_format::OFF_HEADER_CRC..crate::segment_format::OFF_HEADER_CRC + 4]
            .copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            SegmentReader::from_bytes(&wrong_version),
            Err(IndexError::SegmentCorrupt(_))
        ));
    }

    #[test]
    fn a_single_node_segment_round_trips() {
        // The commonest real shape in S1: one commit, one row.
        let row_ids: Vec<u64> = vec![42];
        let (index, reader) = round_trip(1, &row_ids);
        assert_eq!(reader.node_count(), 1);
        assert_eq!(reader.row_id_range(), (42, 42));
        assert_eq!(reader.entry_point(), index.graph.entry_point());
        assert_eq!(reader.vector(0), index.graph.vector(0));
        let mut out = Vec::new();
        reader.neighbors_into(0, 0, &mut out);
        assert!(out.is_empty(), "a lone node has no neighbors");
    }

    #[test]
    fn row_id_iteration_exposes_each_validated_segment_owner_once() {
        let row_ids = vec![7, 11, 19];
        let (_index, reader) = round_trip(row_ids.len(), &row_ids);

        assert_eq!(reader.row_ids().collect::<Vec<_>>(), row_ids);
    }
}
