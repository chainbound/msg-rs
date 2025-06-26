use bytes::Bytes;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
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
        let bytes = compress_prepend_size(data);

        Ok(Bytes::from(bytes))
    }
}

#[derive(Debug, Default)]
pub struct Lz4Decompressor;

impl Decompressor for Lz4Decompressor {
    fn decompress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        let bytes = decompress_size_prepended(data).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Lz4 decompression failed: {e}"))
        })?;

        Ok(Bytes::from(bytes))
    }
}
