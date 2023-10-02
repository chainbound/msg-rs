use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

/// The ID of the auth codec on the wire.
const WIRE_ID: u8 = 0x01;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Invalid wire ID: {0}")]
    WireId(u8),
    #[error("Invalid ACK")]
    InvalidAck,
}

/// Authentication codec.
pub struct Codec {
    state: State,
}

impl Codec {
    /// Creates a new authentication codec for a client. This will put the
    /// codec in the `Ack` state since it will be waiting for an ack.
    pub fn new_client() -> Self {
        Self { state: State::Ack }
    }

    /// Creates a new authentication codec for a server. This will put the
    /// codec in the `AuthReceive` state since it will be waiting for the
    /// client to send its ID.
    pub fn new_server() -> Self {
        Self {
            state: State::AuthReceive,
        }
    }
}

#[derive(Debug, Clone)]
enum State {
    /// Waiting for the client to send its ID
    AuthReceive,
    /// Waiting for the server to send an ACK
    Ack,
}

#[derive(Debug, Clone)]
pub enum Message {
    /// The client sends the ID to the server
    Auth(Bytes),
    /// The server responds with an ACK
    Ack,
}

impl Decoder for Codec {
    type Item = Message;
    type Error = Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.state {
            // We are the server, waiting for the client to send its auth message
            State::AuthReceive => {
                // We need at least 5 bytes to read the wire ID and the auth ID
                if src.is_empty() {
                    return Ok(None);
                }

                // Wire ID check (without advancing the cursor)
                let wire_id = u8::from_be_bytes([src[0]]);
                if wire_id != WIRE_ID {
                    return Err(Error::WireId(wire_id));
                }

                tracing::trace!("wire id: {}", wire_id);

                if src.len() < 4 {
                    tracing::trace!("not enough bytes");
                    return Ok(None);
                }

                let id_size = u32::from_be_bytes([src[1], src[2], src[3], src[4]]);
                if src.len() < id_size as usize {
                    return Ok(None);
                }

                src.advance(1);
                src.advance(4);

                let id = src.split_to(id_size as usize).freeze();
                self.state = State::Ack;
                Ok(Some(Message::Auth(id)))
            }
            // We are the client, and we are waiting for the server to send an ACK
            State::Ack => {
                if src.len() < 2 {
                    return Ok(None);
                }

                // Wire ID check (without advancing the cursor)
                let wire_id = u8::from_be_bytes([src[0]]);
                if wire_id != WIRE_ID {
                    return Err(Error::WireId(wire_id));
                }

                src.advance(1);

                let ack = src.get_u8();

                if ack == 1 {
                    Ok(Some(Message::Ack))
                } else {
                    Err(Error::InvalidAck)
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
                self.state = State::Ack;
                dst.reserve(1 + 4 + id.len());
                dst.put_u8(WIRE_ID);
                dst.put_u32(id.len() as u32);
                dst.put(id);
            }
            // We are the server, and we are sending an ACK to the client
            Message::Ack => {
                dst.reserve(1 + 1);
                dst.put_u8(WIRE_ID);
                dst.put_u8(1);
            }
        }

        Ok(())
    }
}
