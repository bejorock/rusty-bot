//! Caller-supplied, non-persisted native provider credentials.

use crate::ModelError;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialRequest {
    pub provider: String,
    pub model: String,
}

/// A credential deliberately has no serialization implementation. Expose it
/// only to the transport; never include it in history, errors, or logs.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderCredential(String);

impl ProviderCredential {
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ProviderCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProviderCredential([REDACTED])")
    }
}

#[async_trait::async_trait]
pub trait CredentialResolver: Send + Sync {
    async fn resolve(
        &self,
        request: &CredentialRequest,
        cancel: CancellationToken,
    ) -> Result<Option<ProviderCredential>, ModelError>;
}

/// Static credential convenience for callers that do not require refresh.
pub struct StaticCredentialResolver(ProviderCredential);

impl StaticCredentialResolver {
    pub fn new(credential: ProviderCredential) -> Self {
        Self(credential)
    }
}

impl std::fmt::Debug for StaticCredentialResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StaticCredentialResolver([REDACTED])")
    }
}

#[async_trait::async_trait]
impl CredentialResolver for StaticCredentialResolver {
    async fn resolve(
        &self,
        _: &CredentialRequest,
        _: CancellationToken,
    ) -> Result<Option<ProviderCredential>, ModelError> {
        Ok(Some(self.0.clone()))
    }
}
