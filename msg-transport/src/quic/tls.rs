use std::sync::Arc;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::{
    SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, PrivateKeyDer},
};

use crate::quic::ALPN_PROTOCOL;

/// A server certificate verifier that automatically passes all checks.
#[derive(Debug)]
pub(crate) struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::aws_lc_rs::default_provider())))
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Returns a TLS configuration that skips all server verification and doesn't do any client
/// authentication, with the correct ALPN protocol.
pub(crate) fn unsafe_client_config() -> QuicClientConfig {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());

    let mut rustls_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("aws_lc_rs provider supports TLS 1.3")
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();

    rustls_config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
    rustls_config.enable_early_data = true;

    rustls_config.try_into().expect("Valid rustls config")
}

/// Returns a self-signed TLS server configuration that doesn't do any client authentication, with
/// the correct ALPN protocol.
pub(crate) fn tls_server_config() -> QuicServerConfig {
    let (cert_chain, key_der) = self_signed_certificate();
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());

    let mut rustls_config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("aws_lc_rs provider supports TLS 1.3")
        .with_no_client_auth()
        .with_single_cert(cert_chain, key_der)
        .expect("Valid rustls config");

    rustls_config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
    rustls_config.max_early_data_size = u32::MAX;

    rustls_config.try_into().expect("Valid rustls config")
}

/// Generates a self-signed certificate chain and private key.
pub(crate) fn self_signed_certificate() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec![]).expect("Generates valid certificate");
    let cert_der = cert.serialize_der().expect("Serializes certificate");
    let priv_key =
        PrivateKeyDer::try_from(cert.serialize_private_key_der()).expect("Serializes private key");

    (vec![CertificateDer::from(cert_der)], priv_key)
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
