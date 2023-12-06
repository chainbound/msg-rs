use bytes::Bytes;
use std::io;

mod gzip;
mod zstd;
pub use gzip::*;
pub use zstd::*;

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

    #[test]
    fn test_zstd_compression() {
        let compressor = ZstdCompressor::new(6);
        let decompressor = ZstdDecompressor::new();

        let data =
            Bytes::from("hellooooooooooooooooo wwwwwoooooooooooooooooooooooooooooooooooooorld");
        println!("Before: {:?}", data.len());
        let compressed = compressor.compress(&data).unwrap();
        println!("After: {:?}", compressed.len());
        let decompressed = decompressor.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }
}
