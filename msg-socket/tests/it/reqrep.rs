use std::time::Duration;

use bytes::Bytes;
use msg_socket::{RepSocket, ReqSocket};
use msg_transport::{
    tcp::Tcp,
    tcp_tls::{self, TcpTls},
};
use openssl::ssl::{SslAcceptor, SslMethod};
use tokio_stream::StreamExt;

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

#[tokio::test]
async fn reqrep_works() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut rep = RepSocket::new(Tcp::default());
    let mut req = ReqSocket::new(Tcp::default());

    rep.bind("0.0.0.0:0").await.unwrap();

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

#[tokio::test]
async fn reqrep_tls_works() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_config =
        tcp_tls::config::Server::new(helpers::default_acceptor_builder().build().into());
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

/// Test that changing the [`SslAcceptor`] at runtime works and results in not accepting the
/// connection after modification.
#[tokio::test]
async fn reqrep_tls_control_works() {
    let _ = tracing_subscriber::fmt::try_init();

    let server_config =
        tcp_tls::config::Server::new(helpers::default_acceptor_builder().build().into());
    let tcp_tls_server = TcpTls::new_server(server_config);
    let mut rep = RepSocket::new(tcp_tls_server);

    rep.bind("0.0.0.0:0").await.unwrap();

    let domain = "localhost".to_string();
    let ssl_connector = helpers::default_connector_builder().build();
    let tcp_tls_client =
        TcpTls::new_client(tcp_tls::config::Client::new(domain).with_ssl_connector(ssl_connector));
    let mut req = ReqSocket::new(tcp_tls_client);

    req.connect(rep.local_addr().unwrap()).await.unwrap();

    // Happy case: the reply socket should receive one message, and then exit.
    let handle = tokio::spawn(async move {
        if let Some(request) = rep.next().await {
            let msg = request.msg().clone();
            request.respond(msg).unwrap();
        }
        rep
    });

    let hello = Bytes::from_static(b"hello");
    let response = req.request(hello.clone()).await.unwrap();
    assert_eq!(hello, response, "expected {hello:?}, got {response:?}");

    // By dropping the request socket, we terminate the task, and we can try the unhappy case with
    // the configuration change.
    drop(req);
    let mut rep = handle.await.unwrap();

    let domain = "localhost".to_string();
    let ssl_connector = helpers::default_connector_builder().build();
    let tcp_tls_client =
        TcpTls::new_client(tcp_tls::config::Client::new(domain).with_ssl_connector(ssl_connector));
    let mut req = ReqSocket::new(tcp_tls_client);

    // Now we don't set a trusted root certificate, and we swap the acceptor.
    let acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap().build();
    rep.control(tcp_tls::Control::SwapAcceptor(acceptor.into())).await.unwrap();
    req.connect_sync(rep.local_addr().copied().unwrap());

    tokio::spawn(async move {
        if let Some(request) = rep.next().await {
            let msg = request.msg().clone();
            request.respond(msg).unwrap();
        }
    });

    // If we now try to send a message, it will hang because of connection didn't succeed.
    let hello = Bytes::from_static(b"hello");
    tokio::time::timeout(Duration::from_secs(1), req.request(hello.clone())).await.unwrap_err();
}

#[tokio::test]
async fn reqrep_mutual_tls_works() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut acceptor_builder = helpers::default_acceptor_builder();
    // By specifying peer verification mode, we essentially toggle mTLS.
    acceptor_builder.set_verify(
        openssl::ssl::SslVerifyMode::PEER | openssl::ssl::SslVerifyMode::FAIL_IF_NO_PEER_CERT,
    );
    let server_config = tcp_tls::config::Server::new(acceptor_builder.build().into());
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

#[tokio::test]
async fn reqrep_late_bind_works() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut rep = RepSocket::new(Tcp::default());
    let mut req = ReqSocket::new(Tcp::default());

    let local_addr = "localhost:64521";
    req.connect(local_addr).await.unwrap();

    let hello = Bytes::from_static(b"hello");

    let reply = tokio::spawn(async move { req.request(hello.clone()).await.unwrap() });

    tokio::time::sleep(Duration::from_millis(1000)).await;
    rep.bind(local_addr).await.unwrap();

    let msg = rep.next().await.unwrap();
    let payload = msg.msg().clone();
    msg.respond(payload).unwrap();

    let response = reply.await.unwrap();
    let hello = Bytes::from_static(b"hello");
    assert_eq!(hello, response, "expected {hello:?}, got {response:?}");
}
