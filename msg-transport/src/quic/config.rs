use std::{sync::Arc, time::Duration};

use quinn::{congestion::ControllerFactory, IdleTimeout};

use super::tls::{self_signed_certificate, unsafe_client_config};

use msg_common::constants::MiB;

#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint_config: quinn::EndpointConfig,
    pub client_config: quinn::ClientConfig,
    pub server_config: quinn::ServerConfig,
}

#[derive(Debug)]
pub struct ConfigBuilder<C> {
    cc: C,
    initial_mtu: u16,
    max_stream_bandwidth: u32,
    expected_rtt: u32,
    max_idle_timeout: Duration,
    keep_alive_interval: Duration,
}

impl<C> ConfigBuilder<C>
where
    C: ControllerFactory + Default + Send + Sync + 'static,
{
    /// Creates a new [`ConfigBuilder`] with sensible defaults.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            cc: C::default(),
            initial_mtu: 1460,
            max_stream_bandwidth: 50 * MiB,
            expected_rtt: 100,
            max_idle_timeout: Duration::from_secs(60 * 5),
            keep_alive_interval: Duration::from_secs(15),
        }
    }

    /// Sets the initial MTU.
    pub fn initial_mtu(mut self, mtu: u16) -> Self {
        self.initial_mtu = mtu;
        self
    }

    /// Sets the maximum stream bandwidth in bytes per second.
    pub fn max_stream_bandwidth(mut self, bandwidth: u32) -> Self {
        self.max_stream_bandwidth = bandwidth;
        self
    }

    /// Sets the expected round-trip time in milliseconds.
    pub fn expected_rtt(mut self, rtt: u32) -> Self {
        self.expected_rtt = rtt;
        self
    }

    /// Sets the maximum idle timeout.
    pub fn max_idle_timeout(mut self, timeout: Duration) -> Self {
        self.max_idle_timeout = timeout;
        self
    }

    /// Sets the keep-alive interval.
    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = interval;
        self
    }

    /// Sets the congestion controller.
    pub fn congestion_controller(mut self, cc: C) -> Self {
        self.cc = cc;
        self
    }

    /// Builds the QUIC [`Config`].
    pub fn build(self) -> Config {
        let mut transport = quinn::TransportConfig::default();

        // Stream receive window
        let stream_rwnd = self.max_stream_bandwidth / 1000 * self.expected_rtt;

        transport
            .keep_alive_interval(Some(self.keep_alive_interval))
            .max_idle_timeout(Some(
                IdleTimeout::try_from(self.max_idle_timeout).expect("Valid idle timeout"),
            ))
            // Disable datagram support
            .datagram_receive_buffer_size(None)
            .datagram_send_buffer_size(0)
            .max_concurrent_uni_streams(0u32.into())
            .initial_mtu(self.initial_mtu)
            .min_mtu(self.initial_mtu)
            .allow_spin(false)
            .stream_receive_window((8 * stream_rwnd).into())
            .congestion_controller_factory(self.cc)
            .initial_rtt(Duration::from_millis(self.expected_rtt.into()))
            .send_window((8 * stream_rwnd).into());

        let transport = Arc::new(transport);
        let (cert, key) = self_signed_certificate();

        let mut server_config =
            quinn::ServerConfig::with_single_cert(cert, key).expect("Valid rustls config");

        server_config.use_retry(true);
        server_config.transport_config(Arc::clone(&transport));

        let mut client_config = quinn::ClientConfig::new(Arc::new(unsafe_client_config()));

        client_config.transport_config(transport);

        Config { endpoint_config: quinn::EndpointConfig::default(), client_config, server_config }
    }
}

impl Default for Config {
    fn default() -> Self {
        // The expected RTT in ms. This has a big impact on initial performance.
        const EXPECTED_RTT: u32 = 100;
        // The maximum bandwidth we expect to see in bytes per second
        const MAX_STREAM_BANDWIDTH: u32 = MiB * 1000;
        const STREAM_RWND: u32 = MAX_STREAM_BANDWIDTH / 1000 * EXPECTED_RTT;

        const INITIAL_MTU: u16 = 1460;

        // Default initial window is 12000 bytes. This is limited to not overwhelm slow links.
        let mut cc = quinn::congestion::CubicConfig::default();
        // 1 MiB initial window
        // Note that this is a very high initial window outside of private networks.
        // TODO: document this and make it configurable
        cc.initial_window(MiB as u64);

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
            .initial_rtt(Duration::from_millis(EXPECTED_RTT.into()))
            .send_window((8 * STREAM_RWND).into());

        let transport = Arc::new(transport);
        let (cert, key) = self_signed_certificate();

        let mut server_config =
            quinn::ServerConfig::with_single_cert(cert, key).expect("Valid rustls config");

        server_config.use_retry(true);
        server_config.transport_config(Arc::clone(&transport));

        let mut client_config = quinn::ClientConfig::new(Arc::new(unsafe_client_config()));

        client_config.transport_config(transport);

        Self { endpoint_config: quinn::EndpointConfig::default(), client_config, server_config }
    }
}
