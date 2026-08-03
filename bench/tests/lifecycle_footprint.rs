use strata_bench::{ManifestListedFile, SnapshotFootprint, pinned_snapshot_footprint_diagnostics};

fn snapshot(version: u64) -> SnapshotFootprint {
    SnapshotFootprint {
        version,
        manifest_payload_bytes: 1_000 + version,
        row_data_files: vec![
            ManifestListedFile::new("shared.arrow", 10),
            ManifestListedFile::new(format!("row-{version}.arrow"), version),
        ],
        immutable_segment_files: vec![
            ManifestListedFile::new("shared.seg", 20),
            ManifestListedFile::new(format!("segment-{version}.seg"), version),
        ],
    }
}

#[test]
fn direct_footprint_keeps_logical_references_separate_from_unique_physical_files() {
    // Break caught: summing every manifest-listed file reference as a distinct
    // physical payload hides sharing between retained snapshots.
    for (pin_count, expected) in [
        (0, (0, 0, 0, 0, 0, 0)),
        (1, (1_001, 1_001, 11, 11, 21, 21)),
        (4, (4_010, 4_010, 50, 20, 90, 30)),
        (16, (16_136, 16_136, 296, 146, 456, 156)),
        (64, (66_080, 66_080, 2_720, 2_090, 3_360, 2_100)),
    ] {
        let snapshots: Vec<_> = (1..=pin_count).map(snapshot).collect();
        let actual = pinned_snapshot_footprint_diagnostics(&snapshots);
        assert_eq!(
            (
                actual.logical_manifest_payload,
                actual.unique_manifest_payload,
                actual.logical_row_data,
                actual.unique_row_data,
                actual.logical_immutable_segments,
                actual.unique_immutable_segments,
            ),
            expected,
            "pin count {pin_count} must retain the expected logical references and unique physical payloads"
        );
    }
}
