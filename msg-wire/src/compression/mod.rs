use bytes::Bytes;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use std::io::{self, Read, Write};

/// This trait is used to implement message-level compression algorithms for payloads.
/// On outgoing messages, the payload is compressed before being sent using the `compress` method.
pub trait Compressor: Send + Sync + Unpin + 'static {
    /// Compresses a byte slice payload into a `Bytes` object.
    fn compress(&self, data: &[u8]) -> Result<Bytes, io::Error>;
}

/// This trait is used to implement message-level decompression algorithms for payloads.
/// On incoming messages, the payload is decompressed using the `decompress` method.
pub trait Decompressor: Send + Sync + Unpin + 'static {
    /// Decompresses a compressed byte slice into a `Bytes` object.
    fn decompress(&self, data: &[u8]) -> Result<Bytes, io::Error>;
}

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
    fn compress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        // Optimistically allocate the compressed buffer to 1/4 of the original size.
        let mut encoder = GzEncoder::new(
            Vec::with_capacity(data.len() / 4),
            Compression::new(self.level),
        );

        encoder.write_all(data)?;

        let bytes = encoder.finish()?;

        Ok(Bytes::from(bytes))
    }
}

#[derive(Debug, Default)]
pub struct GzipDecompressor;

impl GzipDecompressor {
    pub fn new() -> Self {
        Self
    }
}

impl Decompressor for GzipDecompressor {
    fn decompress(&self, data: &[u8]) -> Result<Bytes, io::Error> {
        let mut decoder = GzDecoder::new(data);

        let mut bytes = Vec::with_capacity(data.len() * 4);
        decoder.read_to_end(&mut bytes)?;

        Ok(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gzip_compression() {
        let compressor = GzipCompressor::new(6);
        let decompressor = GzipDecompressor::new();

        let data =
            Bytes::from("hellooooooooooooooooo wwwwwoooooooooooooooooooooooooooooooooooooorld");
        println!("Before: {:?}", data.len());
        let compressed = compressor.compress(&data).unwrap();
        println!("After: {:?}", compressed.len());
        let decompressed = decompressor.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }
}
