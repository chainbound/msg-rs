use bytes::{Buf, BufMut, Bytes};
use tokio_util::codec::{Decoder, Encoder};

/// Authentication codec
pub struct Codec {
    state: State,
}

impl Codec {
    /// Creates a new authentication codec for a client. This will put the
    pub fn new_client() -> Self {
        Self { state: State::Ack }
    }

    pub fn new_server() -> Self {
        Self {
            state: State::AuthReceive,
        }
    }
}

#[derive(Debug, Clone)]
enum State {
    AuthReceive,
    Ack,
}

pub enum Message {
    /// The client sends the ID to the server
    Auth(Bytes),
    /// The server responds with an ACK
    Ack,
}

impl Decoder for Codec {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.state {
            // We are the server, waiting for the client to send its auth message
            State::AuthReceive => {
                // We need at least 4 bytes to read the ID size
                if src.len() < 4 {
                    return Ok(None);
                }

                let id_size = src.get_u32();
                if src.len() < id_size as usize {
                    return Ok(None);
                }

                let id = src.split_to(id_size as usize).freeze();
                self.state = State::Ack;
                Ok(Some(Message::Auth(id)))
            }
            // We are the client, and we are waiting for the server to send an ACK
            State::Ack => {
                if src.is_empty() {
                    return Ok(None);
                }

                let ack = src.get_u8();

                if ack == 1 {
                    Ok(Some(Message::Ack))
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid ACK",
                    ))
                }
            }
        }
    }
}

impl Encoder<Message> for Codec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        match item {
            // We are the client, and we are sending the ID to the server
            Message::Auth(id) => {
                self.state = State::AuthReceive;

                dst.reserve(id.len());
                dst.put(id);
            }
            // We are the server, and we are sending an ACK to the client
            Message::Ack => {
                dst.reserve(1);
                dst.put_u8(1);
            }
        }

        Ok(())
    }
}
