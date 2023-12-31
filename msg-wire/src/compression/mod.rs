use bytes::Bytes;
use std::io;

mod gzip;
mod snappy;
mod zstd;
pub use gzip::*;
pub use snappy::*;
pub use zstd::*;

/// The possible compression type used for a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Gzip = 1,
    Zstd = 2,
    Snappy = 3,
}

impl TryFrom<u8> for CompressionType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Gzip),
            2 => Ok(CompressionType::Zstd),
            3 => Ok(CompressionType::Snappy),
            _ => Err(value),
        }
    }
}

/// This trait is used to implement message-level compression algorithms for payloads.
/// On outgoing messages, the payload is compressed before being sent using the `compress` method.
pub trait Compressor: Send + Sync + Unpin + 'static {
    /// Returns the compression type assigned to this compressor.
    fn compression_type(&self) -> CompressionType;

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
        let decompressor = GzipDecompressor;

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
        let decompressor = ZstdDecompressor;

        let data =
            Bytes::from("hellooooooooooooooooo wwwwwoooooooooooooooooooooooooooooooooooooorld");
        println!("Before: {:?}", data.len());
        let compressed = compressor.compress(&data).unwrap();
        println!("After: {:?}", compressed.len());
        let decompressed = decompressor.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_snappy_compression() {
        let compressor = SnappyCompressor;
        let decompressor = SnappyDecompressor;

        let data =
            Bytes::from("hellooooooooooooooooo wwwwwoooooooooooooooooooooooooooooooooooooorld");
        println!("Before: {:?}", data.len());
        let compressed = compressor.compress(&data).unwrap();
        println!("After: {:?}", compressed.len());
        let decompressed = decompressor.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
    }

    fn compression_test<C: Compressor>(data: &Bytes, comp: C) -> (std::time::Duration, f64, Bytes) {
        let uncompressed_size = data.len() as f64;
        let start = std::time::Instant::now();

        let compressed = comp.compress(data).unwrap();

        let time = std::time::Instant::now() - start;
        let compressed_size = compressed.len() as f64;
        let shrinkage = uncompressed_size / compressed_size * 100.0;

        (time, shrinkage, compressed)
    }

    fn decompression_test<D: Decompressor>(data: &Bytes, decomp: D) -> std::time::Duration {
        let start = std::time::Instant::now();
        decomp.decompress(data).unwrap();
        std::time::Instant::now() - start
    }

    #[test]
    fn test_compare_compression_algorithms() {
        let data = Bytes::from(
            std::fs::read("../testdata/mainnetCapellaBlock7928030.ssz")
                .expect("failed to read test file"),
        );

        println!("uncompressed data size: {} bytes", data.len());

        let gzip = GzipCompressor::new(6);
        let (gzip_time, gzip_perf, gzip_comp) = compression_test(&data, gzip);
        println!(
            "gzip compression shrank the data by {:.2}% in {:?}",
            gzip_perf, gzip_time
        );

        let zstd = ZstdCompressor::new(6);
        let (zstd_time, zstd_perf, zstd_comp) = compression_test(&data, zstd);
        println!(
            "zstd compression shrank the data by {:.2}% in {:?}",
            zstd_perf, zstd_time
        );

        let snappy = SnappyCompressor;
        let (snappy_time, snappy_perf, snappy_comp) = compression_test(&data, snappy);
        println!(
            "snappy compression shrank the data by {:.2}% in {:?}",
            snappy_perf, snappy_time
        );

        println!("------");

        let gzip = GzipDecompressor;
        let gzip_time = decompression_test(&gzip_comp, gzip);
        println!("gzip decompression took {:?}", gzip_time);

        let zstd = ZstdDecompressor;
        let zstd_time = decompression_test(&zstd_comp, zstd);
        println!("zstd decompression took {:?}", zstd_time);

        let snappy = SnappyDecompressor;
        let snappy_time = decompression_test(&snappy_comp, snappy);
        println!("snappy decompression took {:?}", snappy_time);
    }
}
