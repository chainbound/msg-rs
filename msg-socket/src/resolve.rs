use std::{io, net::SocketAddr};

#[cfg(not(feature = "turmoil"))]
pub(crate) use tokio::net::ToSocketAddrs;
#[cfg(feature = "turmoil")]
pub(crate) use turmoil::ToSocketAddrs;

#[cfg(not(feature = "turmoil"))]
pub(crate) async fn lookup_host(
    addr: impl ToSocketAddrs,
) -> io::Result<impl Iterator<Item = SocketAddr>> {
    tokio::net::lookup_host(addr).await
}

#[cfg(feature = "turmoil")]
pub(crate) async fn lookup_host(
    addr: impl ToSocketAddrs,
) -> io::Result<impl Iterator<Item = SocketAddr>> {
    if !turmoil::in_simulation() {
        return Err(io::Error::other(
            "hostname resolution under the `turmoil` feature requires a running turmoil simulation",
        ));
    }

    turmoil::net::lookup_host(addr).await
}
