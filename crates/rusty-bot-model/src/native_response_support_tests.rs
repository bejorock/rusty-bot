//! In-memory HTTP response fixture shared by provider parser unit tests.
use super::*;

pub(crate) fn test_response(secret: &str) -> NativeResponse {
    NativeResponse {
        response: http::Response::builder()
            .status(200)
            .body(Vec::<u8>::new())
            .unwrap()
            .into(),
        status: 200,
        request_id: Some("unit-request".into()),
        patterns: credential_patterns(secret),
        pending: Vec::new(),
        done: false,
        cancel: CancellationToken::new(),
        bytes_read: 0,
    }
}
