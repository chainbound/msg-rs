use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

/// The ID of the rep/req codec on the wire.
const WIRE_ID: u8 = 0x02;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Invalid wire ID: {0}")]
    WireId(u8),
}

#[derive(Debug, Clone)]
pub struct Message {
    header: Header,
    /// The message payload.
    payload: Bytes,
}

impl Message {
    #[inline]
    pub fn new(id: u32, compression_type: u8, payload: Bytes) -> Self {
        Self { header: Header { id, compression_type, size: payload.len() as u32 }, payload }
    }

    #[inline]
    pub fn id(&self) -> u32 {
        self.header.id
    }

    #[inline]
    pub fn payload_size(&self) -> u32 {
        self.header.size
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.header.len() + self.payload_size() as usize
    }

    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    #[inline]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    #[inline]
    pub fn into_payload(self) -> Bytes {
        self.payload
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Header {
    /// The compression type.
    pub(crate) compression_type: u8,
    /// The message ID.
    pub(crate) id: u32,
    /// The size of the message. Max 4GiB.
    pub(crate) size: u32,
}

impl Header {
    /// Returns the length of the header in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        4 + // id
        4 + // size
        1 // compression type
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn compression_type(&self) -> u8 {
        self.compression_type
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
                    let mut cursor = 0;

                    if src.is_empty() {
                        return Ok(None);
                    }

                    // Wire ID check (without advancing the cursor)
                    let wire_id = u8::from_be_bytes([src[cursor]]);
                    cursor += 1;
                    if wire_id != WIRE_ID {
                        return Err(Error::WireId(wire_id));
                    }

                    // The src is too small to read the compression type
                    if src.len() < cursor + 1 {
                        return Ok(None);
                    }

                    let compression_type = u8::from_be_bytes([src[cursor]]);

                    cursor += 1;

                    if src.len() < cursor + 8 {
                        return Ok(None);
                    }

                    // Only advance when we know we have enough bytes
                    src.advance(cursor);

                    // Construct the header
                    let header =
                        Header { compression_type, id: src.get_u32(), size: src.get_u32() };

                    self.state = State::Payload(header);
                }
                State::Payload(header) => {
                    if src.len() < header.size as usize {
                        return Ok(None);
                    }

                    let payload = src.split_to(header.size as usize);
                    let message = Message { header, payload: payload.freeze() };

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
        dst.reserve(1 + item.header.len() + item.payload_size() as usize);

        dst.put_u8(WIRE_ID);
        dst.put_u8(item.header.compression_type);
        dst.put_u32(item.header.id);
        dst.put_u32(item.header.size);
        dst.put(item.payload);

        Ok(())
    }
}
