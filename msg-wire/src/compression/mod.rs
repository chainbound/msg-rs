use bytes::Bytes;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use std::io::{Read, Write};

/// This trait is used to implement message-level compression algorithms for payloads.
/// On outgoing messages, the payload is compressed before being sent using the `compress` method.
/// On incoming messages, the inverse happens using the `decompress` method.
pub trait Compressor {
    type Error: std::error::Error;

    /// Compresses a byte slice payload into a `Bytes` object.
    fn compress(&mut self, data: &[u8]) -> Result<Bytes, Self::Error>;

    /// Decompresses a compressed byte slice into a `Bytes` object.
    fn decompress(&mut self, data: &[u8]) -> Result<Bytes, Self::Error>;
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
    type Error = std::io::Error;

    fn compress(&mut self, data: &[u8]) -> Result<Bytes, Self::Error> {
        let mut encoder = GzEncoder::new(
            Vec::with_capacity(data.len() / self.level as usize),
            Compression::new(self.level),
        );

        encoder.write_all(data)?;

        let bytes = encoder.finish()?;

        Ok(Bytes::from(bytes))
    }

    fn decompress(&mut self, data: &[u8]) -> Result<Bytes, Self::Error> {
        let mut decoder = GzDecoder::new(data);

        let mut bytes = Vec::with_capacity(data.len() * self.level as usize);
        decoder.read_to_end(&mut bytes)?;

        Ok(Bytes::from(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gzip_compression() {
        let mut compressor = GzipCompressor::new(6);

        let data =
            Bytes::from("hellooooooooooooooooo wwwwwoooooooooooooooooooooooooooooooooooooorld");
        println!("Before: {:?}", data.len());
        let compressed = compressor.compress(&data).unwrap();
        println!("After: {:?}", compressed.len());
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }
}
