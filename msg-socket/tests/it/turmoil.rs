use std::net::{Ipv4Addr, SocketAddr};
#[cfg(feature = "tcp-tls")]
use std::path::PathBuf;

use bytes::Bytes;
use msg_socket::{RepSocket, ReqSocket};
use msg_transport::tcp::Tcp;
#[cfg(feature = "tcp-tls")]
use msg_transport::tcp_tls::{self, TcpTls};
#[cfg(feature = "tcp-tls")]
use openssl::ssl::{
    SslAcceptor, SslAcceptorBuilder, SslConnector, SslConnectorBuilder, SslFiletype, SslMethod,
};
use tokio_stream::StreamExt;
use turmoil::{Builder, IpVersion, Result};

const SERVER_HOST: &str = "server";
const TCP_PORT: u16 = 17_301;
#[cfg(feature = "tcp-tls")]
const TLS_PORT: u16 = 17_302;

fn build_sim() -> turmoil::Sim<'static> {
    let mut builder = Builder::new();
    builder.ip_version(IpVersion::V4);
    builder.build()
}

fn bind_addr(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::UNSPECIFIED, port))
}

#[cfg(feature = "tcp-tls")]
fn certificate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../testdata/certificates")
}

#[cfg(feature = "tcp-tls")]
fn default_acceptor_builder() -> SslAcceptorBuilder {
    let base = certificate_dir();
    let certificate_path = base.join("server-cert.pem");
    let private_key_path = base.join("server-key.pem");
    let ca_certificate_path = base.join("ca-cert.pem");

    let mut acceptor_builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    acceptor_builder.set_certificate_file(certificate_path, SslFiletype::PEM).unwrap();
    acceptor_builder.set_private_key_file(private_key_path, SslFiletype::PEM).unwrap();
    acceptor_builder.set_ca_file(ca_certificate_path).unwrap();
    acceptor_builder
}

#[cfg(feature = "tcp-tls")]
fn default_connector_builder() -> SslConnectorBuilder {
    let base = certificate_dir();
    let certificate_path = base.join("client-cert.pem");
    let private_key_path = base.join("client-key.pem");
    let ca_certificate_path = base.join("ca-cert.pem");

    let mut connector_builder = SslConnector::builder(SslMethod::tls()).unwrap();
    connector_builder.set_certificate_file(certificate_path, SslFiletype::PEM).unwrap();
    connector_builder.set_private_key_file(private_key_path, SslFiletype::PEM).unwrap();
    connector_builder.set_ca_file(ca_certificate_path).unwrap();
    connector_builder
}

#[test]
fn reqrep_tcp_works_in_turmoil() -> Result {
    let _ = tracing_subscriber::fmt::try_init();

    let mut sim = build_sim();

    sim.host(SERVER_HOST, || async {
        let mut rep = RepSocket::new(Tcp::default());
        rep.bind(bind_addr(TCP_PORT)).await.unwrap();

        let request = rep.next().await.unwrap();
        let msg = request.msg().clone();
        request.respond(msg).unwrap();

        Ok(())
    });

    sim.client("client", async {
        let mut req = ReqSocket::new(Tcp::default());
        req.connect(format!("{SERVER_HOST}:{TCP_PORT}")).await.unwrap();

        let hello = Bytes::from_static(b"hello over turmoil");
        let response = req.request(hello.clone()).await.unwrap();
        assert_eq!(hello, response);

        Ok(())
    });

    sim.run()
}

#[cfg(feature = "tcp-tls")]
#[test]
fn reqrep_tcp_tls_works_in_turmoil() -> Result {
    let _ = tracing_subscriber::fmt::try_init();

    let mut sim = build_sim();

    sim.host(SERVER_HOST, || async {
        let server_config = tcp_tls::config::Server::new(default_acceptor_builder().build().into());
        let mut rep = RepSocket::new(TcpTls::new_server(server_config));
        rep.bind(bind_addr(TLS_PORT)).await.unwrap();

        let request = rep.next().await.unwrap();
        let msg = request.msg().clone();
        request.respond(msg).unwrap();

        Ok(())
    });

    sim.client("client", async {
        let domain = "localhost".to_string();
        let ssl_connector = default_connector_builder().build();
        let tcp_tls_client = TcpTls::new_client(
            tcp_tls::config::Client::new(domain).with_ssl_connector(ssl_connector),
        );
        let mut req = ReqSocket::new(tcp_tls_client);
        req.connect(format!("{SERVER_HOST}:{TLS_PORT}")).await.unwrap();

        let hello = Bytes::from_static(b"hello over turmoil tls");
        let response = req.request(hello.clone()).await.unwrap();
        assert_eq!(hello, response);

        Ok(())
    });

    sim.run()
}
