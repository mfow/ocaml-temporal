use std::{fs, path::PathBuf};

use ocaml_temporal_core_bridge::protocol::{
    self, COMPATIBILITY_VERSION, Envelope, MAX_DOCUMENT_BYTES, MAX_PAYLOAD_BYTES, Request,
};

/// Locates a shared fixture from the Rust crate within the repository tree.
fn fixture_path(parts: &[&str]) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.extend(["..", "..", "test", "bridge", "fixtures", "protocol"]);
    path.extend(parts);
    path
}

/// Reads one shared fixture without embedding its potentially malformed bytes
/// in assertion output.
fn fixture(parts: &[&str]) -> String {
    fs::read_to_string(fixture_path(parts)).expect("shared protocol fixture must be readable")
}

/// Proves Rust produces exactly the same normalized envelope bytes as OCaml.
#[test]
fn accepts_and_normalizes_valid_envelopes() {
    for name in ["request", "response", "error", "unicode"] {
        let input = fixture(&["valid", &format!("{name}.input.json")]);
        let expected = fixture(&["valid", &format!("{name}.normalized.json")]);
        let decoded = protocol::decode(&input).expect("valid envelope should decode");
        assert_eq!(protocol::encode(&decoded).unwrap(), expected.trim());
        protocol::decode(expected.trim()).expect("normalized envelope should decode");
    }
}

/// Proves malformed shared envelopes fail, particularly duplicate object keys.
#[test]
fn rejects_invalid_envelopes() {
    for name in [
        "duplicate-envelope",
        "duplicate-body",
        "missing-field",
        "unknown-field",
        "wrong-type",
        "invalid-correlation",
        "unknown-kind",
        "non-integral-number",
        "integer-out-of-range",
        "error-unknown-field",
    ] {
        let input = fixture(&["invalid", &format!("{name}.json")]);
        assert!(protocol::decode(&input).is_err(), "{name} was accepted");
    }
}

/// Exercises canonical padded base64 and closed opaque-payload objects.
#[test]
fn validates_and_normalizes_payloads() {
    let input = fixture(&["valid", "payload.input.json"]);
    let expected = fixture(&["valid", "payload.normalized.json"]);
    let bytes = protocol::decode_payload(&input).expect("payload should decode");
    assert_eq!(bytes, [0, 1, 2, 254, 255]);
    assert_eq!(protocol::encode_payload(&bytes).unwrap(), expected.trim());
    let all_bytes = (0..=u8::MAX).collect::<Vec<_>>();
    assert_eq!(
        protocol::decode_payload(&protocol::encode_payload(&all_bytes).unwrap()).unwrap(),
        all_bytes
    );
    for name in ["payload-invalid-base64", "payload-unknown-field"] {
        assert!(protocol::decode_payload(&fixture(&["invalid", &format!("{name}.json")])).is_err());
    }
}

/// Ensures hostile document, nesting, and decoded-payload sizes fail before use.
#[test]
fn enforces_resource_limits() {
    let prefix = r#"{"kind":"request","correlation_id":"0123456789abcdef0123456789abcdef","operation":"worker.poll","body":"#;
    let deep = format!("{prefix}{}{}{}", "[".repeat(17), "]".repeat(17), "}");
    let long_string = format!("{prefix}{{\"value\":\"{}\"}}}}", "a".repeat(65_537));
    let long_array = format!(
        "{prefix}{{\"values\":[{}]}}}}",
        std::iter::repeat_n("null", 257)
            .collect::<Vec<_>>()
            .join(",")
    );
    assert!(protocol::decode(&deep).is_err());
    assert!(protocol::decode(&long_string).is_err());
    assert!(protocol::decode(&long_array).is_err());
    assert!(protocol::decode(&" ".repeat(MAX_DOCUMENT_BYTES + 1)).is_err());
    assert!(protocol::encode_payload(&vec![0; MAX_PAYLOAD_BYTES + 1]).is_err());
}

/// Checks the once-per-runtime compatibility number and sender-side validation.
#[test]
fn checks_compatibility_and_outgoing_values() {
    protocol::check_compatibility(COMPATIBILITY_VERSION).unwrap();
    assert!(protocol::check_compatibility(u32::MAX).is_err());
    let invalid = Envelope::Request(Request {
        correlation_id: "not-a-correlation-id".to_owned(),
        operation: "worker.poll".to_owned(),
        body: protocol::JsonValue::Object(Vec::new()),
    });
    assert!(protocol::encode(&invalid).is_err());
}
