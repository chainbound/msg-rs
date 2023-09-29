use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct Message {
    header: Header,
    /// The message payload.
    payload: Bytes,
}

impl Message {
    pub fn new(id: u32, payload: Bytes) -> Self {
        Self {
            header: Header {
                id,
                size: payload.len() as u32,
            },
            payload,
        }
    }

    pub fn id(&self) -> u32 {
        self.header.id
    }

    pub fn payload_size(&self) -> u32 {
        self.header.size
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn into_payload(self) -> Bytes {
        self.payload
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Header {
    /// The message ID.
    pub(crate) id: u32,
    /// The size of the message. Max 4GiB.
    pub(crate) size: u32,
}

impl Header {
    /// Returns the length of the header in bytes.
    #[inline]
    pub fn len() -> usize {
        8
    }
}

#[derive(Default)]
enum State {
    #[default]
    Header,
    Payload(Header),
}

#[derive(Default)]
pub struct Codec {
    /// The current state of the decoder.
    state: State,
}

impl Codec {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            match self.state {
                State::Header => {
                    if src.len() < Header::len() {
                        return Ok(None);
                    }

                    // Construct the header
                    let header = Header {
                        id: src.get_u32(),
                        size: src.get_u32(),
                    };

                    self.state = State::Payload(header);
                }
                State::Payload(header) => {
                    if src.len() < header.size as usize {
                        return Ok(None);
                    }

                    let payload = src.split_to(header.size as usize);
                    let message = Message {
                        header,
                        payload: payload.freeze(),
                    };

                    self.state = State::Header;
                    return Ok(Some(message));
                }
            }
        }
    }
}

impl Encoder<Message> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        dst.reserve(8 + item.payload_size() as usize);

        dst.put_u32(item.header.id);
        dst.put_u32(item.header.size);
        dst.put(item.payload);

        Ok(())
    }
}
