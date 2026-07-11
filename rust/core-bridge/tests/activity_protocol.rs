use ocaml_temporal_core_bridge::activity_protocol::{
    ActivityCancel, ActivityCancelReason, ActivityCompletion, ActivityCompletionResult,
    ActivityTask, ActivityTaskVariant, completion_to_core, decode_completion, decode_task,
    encode_completion, encode_task,
};

/// Activity task tokens survive canonical JSON and return to Core unchanged.
#[test]
fn activity_completion_preserves_opaque_task_token() {
    let completion = ActivityCompletion {
        task_token: "AAEC/v8=".to_owned(),
        result: ActivityCompletionResult::Completed { result: None },
    };

    let encoded = encode_completion(&completion).expect("completion should encode");
    assert_eq!(decode_completion(&encoded), Ok(completion.clone()));
    assert_eq!(
        completion_to_core(&completion)
            .expect("completion should convert")
            .task_token,
        vec![0, 1, 2, 254, 255]
    );
}

/// The semantic activity decoder rejects unknown fields and non-canonical or
/// empty tokens before any completion can reach Core.
#[test]
fn activity_documents_are_closed_and_tokens_are_canonical() {
    assert!(
        decode_completion(
            r#"{"task_token":"AA==","result":{"kind":"will_complete_async"},"extra":true}"#
        )
        .is_err()
    );
    assert!(
        decode_completion(r#"{"task_token":"","result":{"kind":"will_complete_async"}}"#).is_err()
    );
    assert!(
        decode_completion(r#"{"task_token":"AA","result":{"kind":"will_complete_async"}}"#)
            .is_err()
    );
}

/// Cancellation tasks retain the stable reason while preserving the single
/// original task-token completion obligation.
#[test]
fn activity_cancellation_task_round_trips() {
    let task = ActivityTask {
        task_token: "dG9rZW4=".to_owned(),
        variant: ActivityTaskVariant::Cancel(ActivityCancel {
            reason: ActivityCancelReason::WorkerShutdown,
            details: None,
        }),
    };

    let encoded = encode_task(&task).expect("task should encode");
    assert_eq!(decode_task(&encoded), Ok(task));
}
