use std::{fmt, sync::Arc};

use openssl::ssl::{SslAcceptor, SslConnector};

/// Configuration for a TCP-TLS client.
#[derive(Debug, Clone)]
pub struct Client {
    /// The domain name for Server Name Indication (SNI) during the TLS handshake.
    pub domain: String,

    /// The SSL connector for performing a TLS handshake with a server.
    pub ssl_connector: Option<SslConnector>,
}

impl Client {
    pub fn new(domain: String) -> Self {
        Self { domain, ssl_connector: None }
    }

    pub fn with_ssl_connector(mut self, ssl_connector: SslConnector) -> Self {
        self.ssl_connector = Some(ssl_connector);
        self
    }
}

/// Configuration for a TCP-TLS server.
#[derive(Clone)]
pub struct Server {
    /// The SSL acceptor for performing TLS handshakes with a client.
    pub ssl_acceptor: Arc<SslAcceptor>,
}

impl Server {
    pub fn new(ssl_acceptor: Arc<SslAcceptor>) -> Self {
        Self { ssl_acceptor }
    }
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server").field("ssl_acceptor", &"SslAcceptor").finish()
    }
}
