use std::sync::Arc;

use super::tls::{self_signed_certificate, unsafe_client_config};

#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint_config: quinn::EndpointConfig,
    pub client_config: quinn::ClientConfig,
    pub server_config: quinn::ServerConfig,
}

impl Default for Config {
    fn default() -> Self {
        let (cert, key) = self_signed_certificate();

        let mut server_config =
            quinn::ServerConfig::with_single_cert(cert, key).expect("Valid rustls config");

        server_config.use_retry(true);

        Self {
            endpoint_config: quinn::EndpointConfig::default(),
            client_config: quinn::ClientConfig::new(Arc::new(unsafe_client_config())),
            server_config,
        }
    }
}
