use std::fmt;
use std::sync::Arc;

pub const TENANT_HEADER: &str = "X-Scope-OrgID";
/// gRPC metadata keys are lowercase.
pub const TENANT_METADATA_KEY: &str = "x-scope-orgid";
pub const MAX_TENANT_ID_BYTES: usize = 64;

/// A validated tenant identifier.
///
/// The raw value arrives from an untrusted header and ends up in object-store
/// keys and local filesystem paths, so validation is applied once here and
/// the rest of the engine may assume every `TenantId` is already safe. Which
/// tenants are *served* is the pushed policy set's decision
/// ([`crate::tenant_policy::TenantPolicy::is_tenant_allowed`]): with
/// per-tenant policy enabled, only tenants the control plane has pushed a
/// policy for are accepted.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TenantId(Arc<str>);

impl TenantId {
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.is_empty() {
            return Err("tenant id must not be empty".to_string());
        }
        if raw.len() > MAX_TENANT_ID_BYTES {
            return Err(format!(
                "tenant id is {} bytes, exceeding the maximum of {MAX_TENANT_ID_BYTES}",
                raw.len()
            ));
        }
        if let Some(invalid) = raw
            .chars()
            .find(|c| !c.is_ascii_alphanumeric() && *c != '_' && *c != '-')
        {
            return Err(format!(
                "tenant id contains the unsupported character {invalid:?}; \
only [a-zA-Z0-9_-] is accepted"
            ));
        }
        Ok(Self(Arc::from(raw)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TenantId({})", self.0)
    }
}

impl serde::Serialize for TenantId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for TenantId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// What to do when a request carries no `X-Scope-OrgID`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MissingTenantPolicy {
    /// Attribute the request to `Config::default_tenant`.
    UseDefault,
    /// Reject the request.
    Reject,
}

impl std::str::FromStr for MissingTenantPolicy {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "default" | "use_default" | "accept" => Ok(Self::UseDefault),
            "reject" | "require" => Ok(Self::Reject),
            other => Err(format!("expected 'default' or 'reject', got {other:?}")),
        }
    }
}

#[derive(Debug)]
pub enum TenantError {
    Missing,
    Invalid(String),
    /// Well-formed, but not one this instance serves.
    NotAllowed(TenantId),
}

impl fmt::Display for TenantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => write!(f, "{TENANT_HEADER} is required but was not supplied"),
            Self::Invalid(reason) => write!(f, "invalid {TENANT_HEADER}: {reason}"),
            // Names the tenant rather than the list: an operator debugging a
            // rejected client needs to see what was sent, and the list is
            // their own configuration.
            Self::NotAllowed(tenant) => {
                write!(f, "tenant {tenant} is not served by this instance")
            }
        }
    }
}

/// Resolve a tenant from an already-extracted header value.
///
/// `raw` is `None` when the header is absent and `Some` when it is present,
/// including when it is present but empty (which is always an error rather
/// than a fallback to the default tenant: an empty value is a client bug, and
/// silently attributing that traffic to another tenant hides it).
pub fn resolve(
    raw: Option<&str>,
    default_tenant: &TenantId,
    policy: MissingTenantPolicy,
    allowed: impl Fn(&TenantId) -> bool,
) -> Result<TenantId, TenantError> {
    let tenant = match raw {
        Some(value) => TenantId::parse(value).map_err(TenantError::Invalid)?,
        None => match policy {
            MissingTenantPolicy::UseDefault => default_tenant.clone(),
            MissingTenantPolicy::Reject => return Err(TenantError::Missing),
        },
    };
    // Checked after parsing so a malformed id is still reported as malformed:
    // the two failures send an operator to different places.
    if !allowed(&tenant) {
        return Err(TenantError::NotAllowed(tenant));
    }
    Ok(tenant)
}

impl TenantError {
    /// An unlisted tenant is a well-formed request this instance declines to
    /// serve, which is 403 rather than 400: the client has nothing to fix.
    pub fn http_status(&self) -> axum::http::StatusCode {
        match self {
            Self::NotAllowed(_) => axum::http::StatusCode::FORBIDDEN,
            Self::Missing | Self::Invalid(_) => axum::http::StatusCode::BAD_REQUEST,
        }
    }

    pub fn into_http(self) -> (axum::http::StatusCode, String) {
        (self.http_status(), self.to_string())
    }

    pub fn into_grpc(self) -> tonic::Status {
        match self {
            Self::NotAllowed(_) => tonic::Status::permission_denied(self.to_string()),
            Self::Missing | Self::Invalid(_) => tonic::Status::invalid_argument(self.to_string()),
        }
    }
}

/// Resolve the tenant for an HTTP request.
pub fn from_headers(
    headers: &axum::http::HeaderMap,
    config: &crate::config::Config,
    tenant_policy: &crate::tenant_policy::TenantPolicy,
) -> Result<TenantId, TenantError> {
    let raw = match headers.get(TENANT_HEADER) {
        Some(value) => Some(
            value
                .to_str()
                .map_err(|_| TenantError::Invalid("value is not valid ASCII".to_string()))?,
        ),
        None => None,
    };
    resolve(
        raw,
        &config.default_tenant,
        config.missing_tenant_policy,
        |tenant| tenant_policy.is_tenant_allowed(tenant),
    )
}

/// Resolve the tenant for an OTLP gRPC request.
pub fn from_grpc_metadata(
    metadata: &tonic::metadata::MetadataMap,
    config: &crate::config::Config,
    tenant_policy: &crate::tenant_policy::TenantPolicy,
) -> Result<TenantId, TenantError> {
    let raw = match metadata.get(TENANT_METADATA_KEY) {
        Some(value) => Some(
            value
                .to_str()
                .map_err(|_| TenantError::Invalid("value is not valid ASCII".to_string()))?,
        ),
        None => None,
    };
    resolve(
        raw,
        &config.default_tenant,
        config.missing_tenant_policy,
        |tenant| tenant_policy.is_tenant_allowed(tenant),
    )
}

/// The tenant every test uses unless it is specifically exercising isolation.
#[cfg(test)]
pub fn test_tenant() -> TenantId {
    TenantId::parse("test-tenant").expect("the test tenant is valid")
}

/// Request headers naming [`test_tenant`], so a test that writes through an
/// HTTP handler can read the data back through another one.
#[cfg(test)]
pub fn test_tenant_headers() -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        TENANT_HEADER,
        test_tenant()
            .as_str()
            .parse()
            .expect("the test tenant is a valid header value"),
    );
    headers
}

/// gRPC metadata naming [`test_tenant`].
#[cfg(test)]
pub fn test_tenant_metadata() -> tonic::metadata::MetadataMap {
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        TENANT_METADATA_KEY,
        test_tenant()
            .as_str()
            .parse()
            .expect("the test tenant is a valid metadata value"),
    );
    metadata
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_the_allowlist_and_rejects_everything_else() {
        assert!(TenantId::parse("fn0-proj_42").is_ok());
        assert!(TenantId::parse(&"a".repeat(MAX_TENANT_ID_BYTES)).is_ok());

        assert!(TenantId::parse("").is_err());
        assert!(TenantId::parse(&"a".repeat(MAX_TENANT_ID_BYTES + 1)).is_err());
        assert!(TenantId::parse("..").is_err());
        assert!(TenantId::parse("a/b").is_err());
        assert!(TenantId::parse("a b").is_err());
        assert!(TenantId::parse("文字").is_err());
        assert!(TenantId::parse("a\0b").is_err());
    }

    /// The registry is checked after parsing, so the two rejections stay
    /// distinct: a malformed id is the client's bug, an unserved one means the
    /// control plane has not onboarded that tenant.
    #[test]
    fn resolve_rejects_a_tenant_the_registry_does_not_serve() {
        let default_tenant = TenantId::parse("default").unwrap();
        let acme = TenantId::parse("acme").unwrap();
        let allowed = |tenant: &TenantId| *tenant == acme;

        assert_eq!(
            resolve(
                Some("acme"),
                &default_tenant,
                MissingTenantPolicy::UseDefault,
                allowed,
            )
            .unwrap(),
            TenantId::parse("acme").unwrap()
        );
        assert!(matches!(
            resolve(
                Some("stranger"),
                &default_tenant,
                MissingTenantPolicy::UseDefault,
                allowed,
            ),
            Err(TenantError::NotAllowed(_))
        ));
        assert!(matches!(
            resolve(
                Some("not a tenant"),
                &default_tenant,
                MissingTenantPolicy::UseDefault,
                allowed,
            ),
            Err(TenantError::Invalid(_))
        ));
        // The default tenant is subject to the registry like any other: a
        // deployment that files headerless requests under it must push a
        // policy for it.
        assert!(matches!(
            resolve(
                None,
                &default_tenant,
                MissingTenantPolicy::UseDefault,
                allowed,
            ),
            Err(TenantError::NotAllowed(_))
        ));
    }

    #[test]
    fn resolve_applies_the_missing_header_policy() {
        let default_tenant = TenantId::parse("default").unwrap();
        let allow_all = |_: &TenantId| true;

        assert_eq!(
            resolve(
                None,
                &default_tenant,
                MissingTenantPolicy::UseDefault,
                allow_all
            )
            .unwrap(),
            default_tenant
        );
        assert!(matches!(
            resolve(
                None,
                &default_tenant,
                MissingTenantPolicy::Reject,
                allow_all
            ),
            Err(TenantError::Missing)
        ));
        // An empty header is a client bug, not an absent header.
        assert!(matches!(
            resolve(
                Some(""),
                &default_tenant,
                MissingTenantPolicy::UseDefault,
                allow_all
            ),
            Err(TenantError::Invalid(_))
        ));
    }
}
