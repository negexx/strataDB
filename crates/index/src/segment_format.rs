//! Constants, field offsets and shared helpers for the on-disk immutable
//! segment format. See
//! `docs/design.md` for the immutable-segment boundary. The format is CSR-flat
//! rather than a mirror of `node_layout.rs`'s mutation-era per-node blocks;
//! `section_len` is `[u32; 4]` so the header
//! fits the specified 128 bytes, and the entry point's level is read from
//! the `levels` section rather than assumed equal to `max_level`.
//!
//! This module is the single source of truth shared by
//! [`crate::segment_writer`] and `crate::segment_reader`: if a constant
//! here changes, both sides change together by construction.

// The format is little-endian by definition, and both the writer and the
// reader reinterpret host-endian bytes via `bytemuck::cast_slice`.
// Supporting a big-endian host would need an explicit byte-swap pass on
// both sides. No such target is in this project's scope, so fail at compile
// time rather than silently emitting a file whose `flags` bit lies about
// its own byte order.
#[cfg(target_endian = "big")]
compile_error!("the Strata segment format requires a little-endian target");

/// File magic. Eight bytes so the header's first `u32` field lands on a
/// natural boundary.
pub(crate) const MAGIC: [u8; 8] = *b"STRTSEG\0";

/// Per-segment (not per-dataset) format version — segments are immutable
/// and never rewritten, so a future writer must still be able to read a
/// segment written today. Recorded in the manifest's
/// `SegmentEntry.format_version` as well as in the file's own header.
pub const SEGMENT_FORMAT_VERSION: u32 = 1;

/// Fixed header size. Also the offset of the first body section, and
/// already 64-byte aligned, which is what lets `row_ids` start immediately
/// after it with no padding.
pub(crate) const HEADER_LEN: usize = 128;

/// `flags` bit 0: the file's multi-byte fields are little-endian.
pub(crate) const FLAG_LITTLE_ENDIAN: u32 = 1;

/// `metric` discriminant for squared-L2 (`crate::distance::L2`). The only
/// metric this crate has; recorded so a future second metric cannot be
/// silently misread.
pub(crate) const METRIC_L2: u8 = 0;

/// `entry_point` sentinel meaning "this segment has no entry point". Never
/// written today (the writer rejects an empty segment outright), but the
/// reader must recognise it rather than treating it as ordinal 4294967295.
// Not read by any production code path yet -- reserved for Task 3's
// `segment_reader`, which recognises this sentinel when loading a header.
#[allow(dead_code)]
pub(crate) const NO_ENTRY_POINT: u32 = u32::MAX;

/// Required alignment of the `vectors` section's start, so a future mmap
/// upgrade can hand out `&[f32]` views with no copy and SIMD-friendly
/// alignment.
pub(crate) const VECTORS_ALIGN: usize = 64;

/// Number of body sections: `row_ids`, `levels`, `adjacency`, `vectors`.
pub(crate) const SECTION_COUNT: usize = 4;

// Named section indices. `encode_segment` (this crate's writer) indexes its
// four-element section arrays positionally rather than through these names,
// so they are unused by any production code path today -- reserved for
// Task 3's `segment_reader`, which names sections when reading a header
// back. `segment_writer`'s own tests already use `SECTION_ADJACENCY`/
// `SECTION_VECTORS`.
#[allow(dead_code)]
pub(crate) const SECTION_ROW_IDS: usize = 0;
#[allow(dead_code)]
pub(crate) const SECTION_LEVELS: usize = 1;
// Used by `segment_writer`'s own tests (non-production); a plain
// `cargo build` (no test cfg) has no other consumer until Task 3.
#[allow(dead_code)]
pub(crate) const SECTION_ADJACENCY: usize = 2;
#[allow(dead_code)]
pub(crate) const SECTION_VECTORS: usize = 3;

// Header field byte offsets. See this module's doc comment and the plan's
// format table; every one of these is < HEADER_LEN by construction.
pub(crate) const OFF_MAGIC: usize = 0;
pub(crate) const OFF_FORMAT_VERSION: usize = 8;
pub(crate) const OFF_FLAGS: usize = 12;
pub(crate) const OFF_NODE_COUNT: usize = 16;
pub(crate) const OFF_DIM: usize = 20;
pub(crate) const OFF_MAX_LEVEL: usize = 24;
pub(crate) const OFF_ENTRY_POINT: usize = 28;
pub(crate) const OFF_METRIC: usize = 32;
pub(crate) const OFF_M: usize = 34;
pub(crate) const OFF_MMAX0: usize = 36;
pub(crate) const OFF_MMAX: usize = 38;
pub(crate) const OFF_EF_CONSTRUCTION: usize = 40;
pub(crate) const OFF_M_L: usize = 48;
pub(crate) const OFF_ROW_ID_MIN: usize = 56;
pub(crate) const OFF_ROW_ID_MAX: usize = 64;
pub(crate) const OFF_SECTION_OFF: usize = 72;
pub(crate) const OFF_SECTION_LEN: usize = 104;
pub(crate) const OFF_BODY_CRC: usize = 120;
pub(crate) const OFF_HEADER_CRC: usize = 124;

/// The HNSW build parameters a segment records so a reader can describe
/// (and a future compactor can reproduce) how it was built. Carried in the
/// header; not consulted during search, which needs only the graph itself.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SegmentParams {
    pub(crate) m: usize,
    pub(crate) mmax0: usize,
    pub(crate) mmax: usize,
    pub(crate) ef_construction: usize,
    pub(crate) m_l: f64,
}

/// Rounds `value` up to the next multiple of `align`, which must be a
/// non-zero power of two. Saturating rather than wrapping: an overflow here
/// would be a section offset past `usize::MAX`, which the caller's own
/// `SegmentTooLarge` bounds already rule out, and saturating keeps this
/// helper total.
pub(crate) fn align_up(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two(), "align must be a power of two");
    value.div_ceil(align).saturating_mul(align)
}

/// 64 bytes of storage with 64-byte alignment — the unit `AlignedBytes`
/// allocates in.
// Not constructed by any production code path yet -- `AlignedBytes` itself
// is reserved for Task 3's `segment_reader` (see that type's doc comment);
// exercised today only by this module's own tests.
#[allow(dead_code)]
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Align64([u8; 64]);

/// An owned byte buffer guaranteed to start on a 64-byte boundary, so
/// `bytemuck::try_cast_slice` can hand out `&[u64]`/`&[u32]`/`&[f32]` views
/// of its aligned sub-ranges without ever failing on alignment. Follows the
/// precedent `node_layout.rs` already sets for this crate's aligned-buffer
/// code (base design doc §1: "one small `AlignedBytes` helper, one
/// `// SAFETY:` comment").
///
/// Deliberately not `Vec<u8>`: `Vec<u8>`'s allocation is only 1-byte
/// aligned, so a `&[f32]` cast of its contents can fail at runtime
/// depending on the allocator's whim — an intermittent, environment-
/// dependent failure, which is the worst possible shape for a durability
/// path.
// Not constructed by any production code path yet -- reserved for Task 3's
// `SegmentReader`, which will own one per loaded segment; exercised today
// only by this module's own tests.
#[allow(dead_code)]
pub(crate) struct AlignedBytes {
    blocks: Vec<Align64>,
    len: usize,
}

// `dead_code`: same "reserved for Task 3" note as the struct above -- none
// of these three accessors has a production caller yet.
// `len_without_is_empty`: an AlignedBytes is only ever built from a whole
// segment file, which is never empty; an `is_empty` accessor would have no
// caller.
#[allow(dead_code, clippy::len_without_is_empty)]
impl AlignedBytes {
    /// Copies `src` into a fresh 64-byte-aligned allocation, zero-padding
    /// the tail of the final block.
    pub(crate) fn from_slice(src: &[u8]) -> Self {
        let block_count = src.len().div_ceil(64).max(1);
        let mut blocks = vec![Align64([0_u8; 64]); block_count];
        // SAFETY: `Align64` is `#[repr(C, align(64))]` around exactly one
        // `[u8; 64]` and therefore has size 64 with no padding, so
        // `blocks`'s allocation is a contiguous, fully-initialized run of
        // `block_count * 64` bytes. `blocks.as_mut_ptr()` is valid for
        // writes over that whole run, is uniquely borrowed here (`&mut
        // self`-equivalent: `blocks` is a fresh local), and `u8` has
        // alignment 1, so the cast can never be misaligned. The slice is
        // dropped before `blocks` is moved into `Self`.
        let dst = unsafe {
            std::slice::from_raw_parts_mut(blocks.as_mut_ptr().cast::<u8>(), block_count * 64)
        };
        // Cannot panic: `dst.len()` is `src.len().div_ceil(64) * 64`, which
        // is always `>= src.len()`.
        if let Some(head) = dst.get_mut(..src.len()) {
            head.copy_from_slice(src);
        }
        Self {
            blocks,
            len: src.len(),
        }
    }

    /// The logical contents — exactly `src.len()` bytes, without the
    /// zero padding `from_slice` added to reach a whole block.
    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: same layout guarantee as `from_slice` — `blocks` is a
        // contiguous run of `blocks.len() * 64` initialized bytes, `u8` has
        // alignment 1, and `self.len <= blocks.len() * 64` by construction
        // (`from_slice` is the only constructor). The returned slice
        // borrows `self`, so it cannot outlive `blocks`.
        unsafe { std::slice::from_raw_parts(self.blocks.as_ptr().cast::<u8>(), self.len) }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn header_field_offsets_all_fit_inside_the_fixed_header() {
        // The one property every `read_*` in the reader relies on to be
        // panic-free after a single `len >= HEADER_LEN` check.
        assert_eq!(OFF_SECTION_OFF + SECTION_COUNT * 8, OFF_SECTION_LEN);
        assert_eq!(OFF_SECTION_LEN + SECTION_COUNT * 4, OFF_BODY_CRC);
        assert_eq!(OFF_BODY_CRC + 4, OFF_HEADER_CRC);
        assert_eq!(OFF_HEADER_CRC + 4, HEADER_LEN);
    }

    #[test]
    fn the_header_is_already_vector_section_aligned() {
        // Why `row_ids` can start at HEADER_LEN with no padding, and why a
        // future mmap upgrade needs no header change.
        assert_eq!(HEADER_LEN % VECTORS_ALIGN, 0);
        assert_eq!(HEADER_LEN % 8, 0);
    }

    #[test]
    fn align_up_rounds_to_the_next_multiple_and_leaves_exact_multiples_alone() {
        assert_eq!(align_up(0, 64), 0);
        assert_eq!(align_up(1, 64), 64);
        assert_eq!(align_up(64, 64), 64);
        assert_eq!(align_up(65, 64), 128);
        assert_eq!(align_up(5, 4), 8);
        assert_eq!(align_up(8, 4), 8);
    }

    #[test]
    fn aligned_bytes_round_trips_its_contents_and_starts_64_byte_aligned() {
        let src: Vec<u8> = (0..100_u8).collect();
        let aligned = AlignedBytes::from_slice(&src);
        assert_eq!(aligned.len(), 100);
        assert_eq!(aligned.as_slice(), src.as_slice());
        assert_eq!(
            aligned.as_slice().as_ptr() as usize % VECTORS_ALIGN,
            0,
            "the whole point of this type"
        );
    }

    #[test]
    fn aligned_bytes_handles_an_empty_input_without_a_zero_sized_allocation() {
        let aligned = AlignedBytes::from_slice(&[]);
        assert_eq!(aligned.len(), 0);
        assert!(aligned.as_slice().is_empty());
    }

    #[test]
    fn aligned_bytes_lets_bytemuck_cast_every_typed_view_the_format_needs() {
        // The property the reader depends on: an aligned base plus
        // aligned-by-construction section offsets means no `try_cast_slice`
        // can fail on alignment.
        let src = vec![0_u8; 256];
        let aligned = AlignedBytes::from_slice(&src);
        let bytes = aligned.as_slice();
        assert!(bytemuck::try_cast_slice::<u8, u64>(&bytes[0..64]).is_ok());
        assert!(bytemuck::try_cast_slice::<u8, u32>(&bytes[64..128]).is_ok());
        assert!(bytemuck::try_cast_slice::<u8, f32>(&bytes[128..256]).is_ok());
    }
}
