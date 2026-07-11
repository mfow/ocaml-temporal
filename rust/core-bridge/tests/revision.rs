use ocaml_temporal_core_bridge::{TEMPORAL_CORE_REVISION, temporal_core_revision};

#[test]
/// Prevents the exported revision accessor from drifting from the pinned value.
fn records_the_pinned_core_revision() {
    assert_eq!(temporal_core_revision(), TEMPORAL_CORE_REVISION);
}
