use std::sync::Arc;

use rustls::client::{ServerCertVerified, ServerCertVerifier};

/// A server certificate verifier that automatically passes all checks.
#[derive(Debug)]
pub(crate) struct SkipServerVerification;

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
}

/// Returns a TLS configuration that skips all server verification and doesn't do any client
/// authentication.
pub(crate) fn unsafe_client_config() -> rustls::ClientConfig {
    rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth()
}

/// Generates a self-signed certificate chain and private key.
pub(crate) fn self_signed_certificate() -> (Vec<rustls::Certificate>, rustls::PrivateKey) {
    let cert = rcgen::generate_simple_self_signed(vec![]).expect("Generates valid certificate");
    let cert_der = cert.serialize_der().expect("Serializes certificate");
    let priv_key = rustls::PrivateKey(cert.serialize_private_key_der());

    (vec![rustls::Certificate(cert_der)], priv_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_self_signed_certificate() {
        let cert = self_signed_certificate();
        assert_eq!(cert.0.len(), 1);
    }
}
