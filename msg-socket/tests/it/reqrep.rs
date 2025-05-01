use bytes::Bytes;
use msg_socket::{RepSocket, ReqSocket};
use msg_transport::tcp::Tcp;
use tokio_stream::StreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_reqrep() {
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

    let response = req.request(Bytes::from_static(b"hello")).await.unwrap();
    tracing::info!("Response: {:?}", response);
}
