use quinn::IdleTimeout;
use std::{sync::Arc, time::Duration};

use super::tls::{self_signed_certificate, unsafe_client_config};
use msg_common::constants::MiB;

#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint_config: quinn::EndpointConfig,
    pub client_config: quinn::ClientConfig,
    pub server_config: quinn::ServerConfig,
}

impl Default for Config {
    fn default() -> Self {
        // The expected RTT in ms
        const EXPECTED_RTT: u32 = 100;
        // The maximum bandwidth we expect to see in bytes per second
        const MAX_STREAM_BANDWIDTH: u32 = MiB * 1000;
        const STREAM_RWND: u32 = MAX_STREAM_BANDWIDTH / 1000 * EXPECTED_RTT;

        const INITIAL_MTU: u16 = 1460;

        // Default initial window is 12000 bytes. This is limited to not overwhelm slow links.
        let mut cc = quinn::congestion::CubicConfig::default();
        // 4 MiB initial window
        // Note that this is a very high initial window outside of private networks.
        // TODO: document this and make it configurable
        cc.initial_window((MiB * 4) as u64);

        let mut transport = quinn::TransportConfig::default();

        transport
            .keep_alive_interval(Some(Duration::from_secs(10)))
            .max_idle_timeout(Some(
                IdleTimeout::try_from(Duration::from_secs(60 * 5)).expect("Valid idle timeout"),
            ))
            // Disable datagram support
            .datagram_receive_buffer_size(None)
            .datagram_send_buffer_size(0)
            .max_concurrent_uni_streams(0u32.into())
            .initial_mtu(INITIAL_MTU)
            .min_mtu(INITIAL_MTU)
            .allow_spin(false)
            .stream_receive_window((8 * STREAM_RWND).into())
            .congestion_controller_factory(Arc::new(cc))
            .send_window((8 * STREAM_RWND).into());

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
