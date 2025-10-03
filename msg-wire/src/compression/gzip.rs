use bytes::Bytes;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use std::io::{self, Read, Write};

use super::{CompressionType, Compressor, Decompressor};

/// A compressor that uses the gzip algorithm.
pub struct GzipCompressor {
    level: u32,
}

impl GzipCompressor {
    /// Creates a new gzip compressor with the given compression level (0-9).
    pub fn new(level: u32) -> Self {
        Self { level }
    }
}

impl Compressor for GzipCompressor {
    fn compression_type(&self) -> CompressionType {
        CompressionType::Gzip
    }

    fn compress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        // Optimistically allocate the compressed buffer to 1/4 of the original size.
        let mut encoder =
            GzEncoder::new(Vec::with_capacity(data.len() / 4), Compression::new(self.level));

        encoder.write_all(data)?;

        let bytes = encoder.finish()?;

        Ok(Bytes::from(bytes))
    }
}

#[derive(Debug, Default)]
pub struct GzipDecompressor;

impl Decompressor for GzipDecompressor {
    fn decompress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        let mut decoder = GzDecoder::new(data);

        let mut bytes = Vec::with_capacity(data.len() * 4);
        decoder.read_to_end(&mut bytes)?;

        Ok(Bytes::from(bytes))
    }
}
