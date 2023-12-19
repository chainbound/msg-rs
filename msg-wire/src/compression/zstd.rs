use bytes::Bytes;
use std::io;
use zstd::{decode_all, stream::encode_all};

use super::{CompressionType, Compressor, Decompressor};

pub struct ZstdCompressor {
    level: i32,
}

impl ZstdCompressor {
    /// Creates a new zstd compressor with the given compression level (0-9).
    /// Default used by zstd is 3.
    pub fn new(level: i32) -> Self {
        Self { level }
    }
}

impl Compressor for ZstdCompressor {
    fn compression_type(&self) -> CompressionType {
        CompressionType::Zstd
    }

    fn compress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        let compressed = encode_all(data, self.level)?;

        Ok(Bytes::from(compressed))
    }
}

#[derive(Debug, Default)]
pub struct ZstdDecompressor;

impl ZstdDecompressor {
    #[inline]
    pub fn new() -> Self {
        Self
    }
}

impl Decompressor for ZstdDecompressor {
    fn decompress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        let decompressed = decode_all(data)?;

        Ok(Bytes::from(decompressed))
    }
}
