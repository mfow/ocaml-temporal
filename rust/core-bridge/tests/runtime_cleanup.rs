use std::ptr;
use std::time::{Duration, Instant};

use ocaml_temporal_core_bridge::{
    Result as AbiResult, STATUS_OK, ocaml_temporal_core_v2_result_free,
    ocaml_temporal_core_v2_runtime_dispose, ocaml_temporal_core_v2_runtime_new,
    test_runtime_cleanup_counts,
};

/// Proves the nonblocking finalizer path eventually runs Core's destructor.
///
/// This lives in its own integration-test process so no parallel runtime test
/// can advance the process-local counters and accidentally satisfy the check.
#[test]
fn asynchronous_disposal_completes_without_leaking_core() {
    let (created_before, cleaned_before) = test_runtime_cleanup_counts();
    let mut runtime = ptr::null_mut();
    let mut result = AbiResult::default();

    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_new(&mut runtime, &mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_result_free(&mut result) },
        STATUS_OK
    );
    assert_eq!(
        unsafe { ocaml_temporal_core_v2_runtime_dispose(&mut runtime) },
        STATUS_OK
    );
    assert!(runtime.is_null());

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (created, cleaned) = test_runtime_cleanup_counts();
        if created == created_before + 1 && cleaned == cleaned_before + 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "runtime cleanup did not complete: created {created_before}->{created}, cleaned {cleaned_before}->{cleaned}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
