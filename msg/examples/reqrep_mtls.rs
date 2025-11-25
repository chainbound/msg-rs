//! An example of a REQ/REP socket pair communicating over TCP with client authentication i.e.
//! mutual TLS (mTLS).

use bytes::Bytes;
use msg::{RepSocket, ReqSocket, tcp_tls::TcpTls};
use tokio_stream::StreamExt as _;

/// Helper functions.
mod helpers {
    use std::{path::PathBuf, str::FromStr as _};

    use openssl::ssl::{
        SslAcceptor, SslAcceptorBuilder, SslConnector, SslConnectorBuilder, SslFiletype, SslMethod,
    };

    /// Creates a default SSL acceptor builder for testing, with a trusted CA.
    pub fn default_acceptor_builder() -> SslAcceptorBuilder {
        let certificate_path =
            PathBuf::from_str("../testdata/certificates/server-cert.pem").unwrap();
        let private_key_path =
            PathBuf::from_str("../testdata/certificates/server-key.pem").unwrap();
        let ca_certificate_path =
            PathBuf::from_str("../testdata/certificates/ca-cert.pem").unwrap();

        assert!(certificate_path.exists(), "Certificate file does not exist");
        assert!(private_key_path.exists(), "Private key file does not exist");
        assert!(ca_certificate_path.exists(), "CA Certificate file does not exist");

        let mut acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
        acceptor_builder.set_certificate_file(certificate_path, SslFiletype::PEM).unwrap();
        acceptor_builder.set_private_key_file(private_key_path, SslFiletype::PEM).unwrap();
        acceptor_builder.set_ca_file(ca_certificate_path).unwrap();
        acceptor_builder
    }

    /// Creates a default SSL connector builder for testing, with a trusted CA.
    /// It also has client certificate and private key set for mTLS testing.
    pub fn default_connector_builder() -> SslConnectorBuilder {
        let certificate_path =
            PathBuf::from_str("../testdata/certificates/client-cert.pem").unwrap();
        let private_key_path =
            PathBuf::from_str("../testdata/certificates/client-key.pem").unwrap();
        let ca_certificate_path =
            PathBuf::from_str("../testdata/certificates/ca-cert.pem").unwrap();

        assert!(certificate_path.exists(), "Certificate file does not exist");
        assert!(private_key_path.exists(), "Private key file does not exist");
        assert!(ca_certificate_path.exists(), "CA Certificate file does not exist");

        let mut connector_builder = SslConnector::builder(SslMethod::tls()).unwrap();
        connector_builder.set_certificate_file(certificate_path, SslFiletype::PEM).unwrap();
        connector_builder.set_private_key_file(private_key_path, SslFiletype::PEM).unwrap();
        connector_builder.set_ca_file(ca_certificate_path).unwrap();

        connector_builder
    }
}

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut acceptor_builder = helpers::default_acceptor_builder();
    // By specifying peer verification mode, we essentially toggle mTLS.
    acceptor_builder.set_verify(
        openssl::ssl::SslVerifyMode::PEER | openssl::ssl::SslVerifyMode::FAIL_IF_NO_PEER_CERT,
    );
    let server_config = tcp_tls::config::Server::new(acceptor_builder.build());
    let tcp_tls_server = TcpTls::new_server(server_config);
    let mut rep = RepSocket::new(tcp_tls_server);

    rep.bind("0.0.0.0:0").await.unwrap();

    let domain = "localhost".to_string();
    let ssl_connector = helpers::default_connector_builder().build();
    let tcp_tls_client =
        TcpTls::new_client(tcp_tls::config::Client::new(domain).with_ssl_connector(ssl_connector));
    let mut req = ReqSocket::new(tcp_tls_client);

    req.connect(rep.local_addr().unwrap()).await.unwrap();

    tokio::spawn(async move {
        while let Some(request) = rep.next().await {
            let msg = request.msg().clone();
            request.respond(msg).unwrap();
        }
    });

    let hello = Bytes::from_static(b"hello");
    let response = req.request(hello.clone()).await.unwrap();
    assert_eq!(hello, response, "expected {hello:?}, got {response:?}");
}
