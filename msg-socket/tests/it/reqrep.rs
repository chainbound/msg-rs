use std::time::Duration;

use bytes::Bytes;
use msg_socket::{RepSocket, ReqError, ReqOptions, ReqSocket};
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

/// Tests that the high-water mark for pending requests is enforced.
/// When HWM is reached, new requests should fail with `HighWaterMarkReached` error.
#[tokio::test]
async fn reqrep_hwm_reached() {
    let _ = tracing_subscriber::fmt::try_init();

    const HWM: usize = 2;

    let mut rep = RepSocket::new(Tcp::default());
    // Set HWM for pending requests, with a longer timeout so tests don't fail due to timeout
    let options = ReqOptions::default()
        .with_pending_requests_hwm(HWM)
        .with_timeout(Duration::from_secs(30));
    let mut req = ReqSocket::with_options(Tcp::default(), options);

    rep.bind("0.0.0.0:0").await.unwrap();
    req.connect(rep.local_addr().unwrap()).await.unwrap();

    // Give time for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Use a channel to coordinate: we'll send requests but NOT respond until we verify HWM
    let (respond_tx, mut respond_rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn the rep handler that waits for signal before responding
    let rep_handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        // Collect requests until we get the signal
        loop {
            tokio::select! {
                Some(request) = rep.next() => {
                    requests.push(request);
                }
                _ = &mut respond_rx => {
                    // Respond to all collected requests
                    for request in requests {
                        request.respond(Bytes::from("response")).unwrap();
                    }
                    break;
                }
            }
        }
    });

    // Share req via Arc for concurrent access
    let req = std::sync::Arc::new(req);

    // Spawn HWM requests that should succeed, plus one that should fail
    let mut success_receivers = Vec::new();
    for i in 0..HWM {
        let (tx, rx) = tokio::sync::oneshot::channel();
        success_receivers.push(rx);
        let req = std::sync::Arc::clone(&req);
        tokio::spawn(async move {
            let result = req.request(Bytes::from(format!("request{}", i))).await;
            let _ = tx.send(result);
        });
    }

    // Give time for requests to be queued
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn one more request - this should hit HWM
    let (fail_tx, fail_rx) = tokio::sync::oneshot::channel();
    tokio::spawn({
        let req = std::sync::Arc::clone(&req);
        async move {
            let result = req.request(Bytes::from("request_over_hwm")).await;
            let _ = fail_tx.send(result);
        }
    });

    // Wait a bit for the request to be processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The request over HWM should have failed
    let result = fail_rx.await.unwrap();
    assert!(
        matches!(result, Err(ReqError::HighWaterMarkReached(h)) if h == HWM),
        "Expected HWM error, got: {:?}",
        result
    );

    // Now signal rep to respond
    respond_tx.send(()).unwrap();

    // All requests within HWM should succeed
    for (i, rx) in success_receivers.into_iter().enumerate() {
        let result = rx.await.unwrap();
        assert!(result.is_ok(), "Request {} failed: {:?}", i, result);
    }

    rep_handle.await.unwrap();
}
