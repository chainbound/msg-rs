use bytes::Bytes;
use lz4_flex::{compress, decompress};
use std::io;

use super::{CompressionType, Compressor, Decompressor};

/// A compressor that uses the LZ4 algorithm.
#[derive(Default)]
pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compression_type(&self) -> CompressionType {
        CompressionType::Lz4
    }

    fn compress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        let bytes = compress(data);

        Ok(Bytes::from(bytes))
    }
}

#[derive(Debug, Default)]
pub struct Lz4Decompressor;

impl Decompressor for Lz4Decompressor {
    fn decompress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        // Usually the Lz4 compression ratio is 2.1x. So 4x should be plenty.
        let min_uncompressed_size = data.len() * 4;
        let bytes = decompress(data, min_uncompressed_size).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Lz4 decompression failed: {}", e),
            )
        })?;

        Ok(Bytes::from(bytes))
    }
}
