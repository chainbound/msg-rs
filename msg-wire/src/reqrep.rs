use std::io::IoSlice;

use bytes::{Buf, BufMut, Bytes, BytesMut};
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
    #[error("Failed to decompress message")]
    Decompression,
}

#[derive(Debug, Clone)]
pub struct Message {
    /// The message header.
    header: Header,
    /// The message header bytes.
    header_bytes: Bytes,
    /// The message payload.
    payload: Bytes,
}

impl Message {
    #[inline]
    pub fn new(id: u32, compression_type: u8, payload: Bytes) -> Self {
        let mut buf = BytesMut::with_capacity(Header::len());
        buf.put_u8(WIRE_ID);
        buf.put_u8(compression_type);
        buf.put_u32(id);
        buf.put_u32(payload.len() as u32);
        Self {
            header: Header { id, compression_type, size: payload.len() as u32 },
            header_bytes: buf.freeze(),
            payload,
        }
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
        Header::len() + self.payload_size() as usize
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
    pub const fn len() -> usize {
        4 + // id
        4 + // size
        1 // compression type
    }

    #[inline]
    pub fn compression_type(&self) -> u8 {
        self.compression_type
    }
}

impl Buf for Message {
    fn remaining(&self) -> usize {
        self.header_bytes.remaining() + self.payload.remaining()
    }

    fn chunk(&self) -> &[u8] {
        if self.header_bytes.has_remaining() {
            self.header_bytes.chunk()
        } else {
            self.payload.chunk()
        }
    }

    fn advance(&mut self, cnt: usize) {
        // Advance the cursor across chunk boundaries.
        let header_rem = self.header_bytes.remaining();
        if cnt <= header_rem {
            self.header_bytes.advance(cnt);
        } else {
            self.header_bytes.advance(header_rem);
            self.payload.advance(cnt - header_rem);
        }
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut n = 0;

        // Write the header chunk
        if self.header_bytes.has_remaining() && n < dst.len() {
            dst[n] = IoSlice::new(self.header_bytes.chunk());
            n += 1;
        }

        // Write the payload chunk
        if self.payload.has_remaining() && n < dst.len() {
            dst[n] = IoSlice::new(self.payload.chunk());
            n += 1;
        }

        n
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
                    let message =
                        Message::new(header.id, header.compression_type, payload.freeze());

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
        dst.reserve(1 + Header::len() + item.payload_size() as usize);

        dst.put_u8(WIRE_ID);
        dst.put_u8(item.header.compression_type);
        dst.put_u32(item.header.id);
        dst.put_u32(item.header.size);
        dst.put(item.payload);

        Ok(())
    }
}
