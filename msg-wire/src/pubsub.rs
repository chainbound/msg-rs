use core::fmt;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

use msg_common::unix_micros;

/// The ID of the pub/sub codec on the wire.
const WIRE_ID: u8 = 0x03;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("Invalid wire ID: {0}")]
    WireId(u8),
}

#[derive(Clone)]
pub struct Message {
    header: Header,
    /// The message payload.
    payload: Bytes,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg = f.debug_struct("Message");
        dbg.field("seq", &self.seq());
        dbg.field("topic", &self.topic());
        dbg.field("timestamp", &self.timestamp());
        dbg.field("compression_type", &self.header.compression_type);
        dbg.field("size", &self.size());
        dbg.finish()
    }
}

impl Message {
    /// Creates a new message with the given sequence number, topic, and payload.
    /// If the payload is empty, the server will interpret this as a subscription toggle
    /// for the given topic. The timestamp is set to the current UNIX timestamp in microseconds.
    ///
    /// # Panics
    /// Panics if the topic is larger than 65535 bytes.
    #[inline]
    pub fn new(seq: u32, topic: Bytes, payload: Bytes, compression_type: u8) -> Self {
        Self {
            header: Header {
                compression_type,
                topic_size: u16::try_from(topic.len()).expect("Topic too large, max 65535 bytes"),
                topic,
                timestamp: unix_micros(),
                seq,
                size: payload.len() as u32,
            },
            payload,
        }
    }

    /// Creates a new subscribe message for the given topic. The topic is prefixed with
    /// `MSG.SUB.`.
    #[inline]
    pub fn new_sub(topic: Bytes) -> Self {
        let mut prefix = BytesMut::from("MSG.SUB.");
        prefix.put(topic);
        Self::new(0, prefix.freeze(), Bytes::new(), 0)
    }

    /// Creates a new unsubscribe message for the given topic. The topic is prefixed with
    /// `MSG.UNSUB.`.
    #[inline]
    pub fn new_unsub(topic: Bytes) -> Self {
        let mut prefix = BytesMut::from("MSG.UNSUB.");
        prefix.put(topic);
        Self::new(0, prefix.freeze(), Bytes::new(), 0)
    }

    #[inline]
    pub fn seq(&self) -> u32 {
        self.header.seq
    }

    #[inline]
    pub fn payload_size(&self) -> u32 {
        self.header.size
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.header.timestamp
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.header.len() + self.payload_size() as usize
    }

    #[inline]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    #[inline]
    pub fn into_payload(self) -> Bytes {
        self.payload
    }

    #[inline]
    pub fn into_parts(self) -> (Bytes, Bytes) {
        (self.header.topic, self.payload)
    }

    #[inline]
    pub fn compression_type(&self) -> u8 {
        self.header.compression_type
    }

    #[inline]
    pub fn topic(&self) -> &Bytes {
        &self.header.topic
    }
}

#[derive(Debug, Clone)]
pub struct Header {
    /// Compression type used for the message payload.
    pub(crate) compression_type: u8,
    /// Size of the topic in bytes.
    pub(crate) topic_size: u16,
    /// The actual topic.
    pub(crate) topic: Bytes,
    /// The UNIX timestamp in microseconds.
    pub(crate) timestamp: u64,
    /// The message sequence number.
    pub(crate) seq: u32,
    /// The size of the message. Max 4GiB.
    pub(crate) size: u32,
}

impl Header {
    /// Returns the length of the header in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        8 + // u64
        4 + // u32 
        4 + // u32 
        2 + // u16
        1 + // u8 
        self.topic_size as usize
    }

    pub fn is_empty(&self) -> bool {
        self.topic_size == 0
    }
}

#[derive(Default)]
enum State {
    #[default]
    Header,
    Payload(Option<Header>),
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
                    // Keeps track of the cursor position in the buffer
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

                    // The src is too small to read the topic size
                    if src.len() < cursor + 2 {
                        return Ok(None);
                    }

                    let topic_size = u16::from_be_bytes([src[cursor], src[cursor + 1]]);

                    cursor += 2;

                    // We don't have enough bytes to read the topic and the rest of the data
                    // (timestamp u64, seq u32, size u32)
                    if src.len() < cursor + topic_size as usize + 8 + 8 {
                        return Ok(None);
                    }

                    // Advance to the start of the topic bytes
                    src.advance(cursor);

                    let topic = src.split_to(topic_size as usize).freeze();

                    // Construct the header
                    let header = Header {
                        compression_type,
                        topic_size,
                        topic,
                        timestamp: src.get_u64(),
                        seq: src.get_u32(),
                        size: src.get_u32(),
                    };

                    self.state = State::Payload(Some(header));
                }
                State::Payload(ref mut header) => {
                    if src.len() < header.as_ref().unwrap().size as usize {
                        return Ok(None);
                    }

                    let header = header.take().unwrap();

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
        // Reserve enough space for the wire ID, the header, and the payload
        dst.reserve(1 + item.header.len() + item.payload_size() as usize);

        dst.put_u8(WIRE_ID);
        dst.put_u8(item.header.compression_type);
        dst.put_u16(item.header.topic_size);
        dst.put(item.header.topic);
        dst.put_u64(item.header.timestamp);
        dst.put_u32(item.header.seq);
        dst.put_u32(item.header.size);
        dst.put(item.payload);

        Ok(())
    }
}
