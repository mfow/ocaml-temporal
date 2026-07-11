use ocaml_temporal_core_bridge::{TEMPORAL_CORE_REVISION, temporal_core_revision};

#[test]
fn records_the_pinned_core_revision() {
    assert_eq!(temporal_core_revision(), TEMPORAL_CORE_REVISION);
}
