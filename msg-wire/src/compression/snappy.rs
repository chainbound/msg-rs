use bytes::Bytes;
use snap::raw::{Decoder, Encoder};
use std::io;

use super::{CompressionType, Compressor, Decompressor};

/// A compressor that uses the Snappy algorithm.
#[derive(Default)]
pub struct SnappyCompressor;

impl Compressor for SnappyCompressor {
    fn compression_type(&self) -> CompressionType {
        CompressionType::Snappy
    }

    fn compress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        let mut encoder = Encoder::new();

        let bytes = encoder.compress_vec(data)?;

        Ok(Bytes::from(bytes))
    }
}

#[derive(Debug, Default)]
pub struct SnappyDecompressor;

impl Decompressor for SnappyDecompressor {
    fn decompress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        let mut decoder = Decoder::new();

        let bytes = decoder.decompress_vec(data)?;

        Ok(Bytes::from(bytes))
    }
}
