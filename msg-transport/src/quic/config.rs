use std::{sync::Arc, time::Duration};

use quinn::IdleTimeout;

use super::tls::{self_signed_certificate, unsafe_client_config};

#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint_config: quinn::EndpointConfig,
    pub client_config: quinn::ClientConfig,
    pub server_config: quinn::ServerConfig,
}

impl Default for Config {
    fn default() -> Self {
        let mut transport = quinn::TransportConfig::default();
        transport
            .keep_alive_interval(Some(Duration::from_secs(10)))
            .max_idle_timeout(Some(
                IdleTimeout::try_from(Duration::from_secs(60 * 5)).expect("Valid idle timeout"),
            ))
            // Disable datagram support
            .datagram_receive_buffer_size(None)
            .datagram_send_buffer_size(0);

        let transport = Arc::new(transport);
        let (cert, key) = self_signed_certificate();

        let mut server_config =
            quinn::ServerConfig::with_single_cert(cert, key).expect("Valid rustls config");

        server_config.use_retry(true);
        server_config.transport_config(Arc::clone(&transport));

        let mut client_config = quinn::ClientConfig::new(Arc::new(unsafe_client_config()));

        client_config.transport_config(transport);

        Self {
            endpoint_config: quinn::EndpointConfig::default(),
            client_config,
            server_config,
        }
    }
}
