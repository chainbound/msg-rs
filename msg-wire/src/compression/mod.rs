use bytes::Bytes;
use std::io;

mod gzip;
mod lz4;
mod snappy;
mod zstd;
pub use gzip::*;
pub use lz4::*;
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
    Lz4 = 4,
}

impl TryFrom<u8> for CompressionType {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Gzip),
            2 => Ok(CompressionType::Zstd),
            3 => Ok(CompressionType::Snappy),
            4 => Ok(CompressionType::Lz4),
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

/// Tries to decompress a payload using the given compression type.
/// If the compression type is `None`, the payload is returned as-is.
///
/// ## Errors
/// - If the compression type is not supported
/// - If the payload is invalid
/// - If the decompression fails
pub fn try_decompress_payload(compression_type: u8, data: Bytes) -> Result<Bytes, io::Error> {
    match CompressionType::try_from(compression_type) {
        Ok(supported_compression_type) => match supported_compression_type {
            CompressionType::None => Ok(data),
            CompressionType::Gzip => GzipDecompressor.decompress(data.as_ref()),
            CompressionType::Zstd => ZstdDecompressor.decompress(data.as_ref()),
            CompressionType::Snappy => SnappyDecompressor.decompress(data.as_ref()),
            CompressionType::Lz4 => Lz4Decompressor.decompress(data.as_ref()),
        },
        Err(unsupported_compression_type) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported compression type: {unsupported_compression_type}"),
        )),
    }
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

    #[test]
    fn test_lz4_compression() {
        let compressor = Lz4Compressor;
        let decompressor = Lz4Decompressor;

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
    fn test_compare_compression_algorithms_ssz_block() {
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

        let lz4 = Lz4Compressor;
        let (lz4_time, lz4_perf, lz4_comp) = compression_test(&data, lz4);
        println!(
            "lz4 compression shrank the data by {:.2}% in {:?}",
            lz4_perf, lz4_time
        );

        println!("------ SSZ BLOCK -------");

        let gzip = GzipDecompressor;
        let gzip_time = decompression_test(&gzip_comp, gzip);
        println!("gzip decompression took {:?}", gzip_time);

        let zstd = ZstdDecompressor;
        let zstd_time = decompression_test(&zstd_comp, zstd);
        println!("zstd decompression took {:?}", zstd_time);

        let snappy = SnappyDecompressor;
        let snappy_time = decompression_test(&snappy_comp, snappy);
        println!("snappy decompression took {:?}", snappy_time);

        let lz4 = Lz4Decompressor;
        let lz4_time = decompression_test(&lz4_comp, lz4);
        println!("lz4 decompression took {:?}", lz4_time);
    }

    #[test]
    fn test_compare_compression_algorithms_blob_tx() {
        let data = Bytes::from(
            std::fs::read("../testdata/blobTransactionRaw").expect("failed to read test file"),
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

        let lz4 = Lz4Compressor;
        let (lz4_time, lz4_perf, lz4_comp) = compression_test(&data, lz4);
        println!(
            "lz4 compression shrank the data by {:.2}% in {:?}",
            lz4_perf, lz4_time
        );

        println!("------ BLOB TX ------");

        let gzip = GzipDecompressor;
        let gzip_time = decompression_test(&gzip_comp, gzip);
        println!("gzip decompression took {:?}", gzip_time);

        let zstd = ZstdDecompressor;
        let zstd_time = decompression_test(&zstd_comp, zstd);
        println!("zstd decompression took {:?}", zstd_time);

        let snappy = SnappyDecompressor;
        let snappy_time = decompression_test(&snappy_comp, snappy);
        println!("snappy decompression took {:?}", snappy_time);

        let lz4 = Lz4Decompressor;
        let lz4_time = decompression_test(&lz4_comp, lz4);
        println!("lz4 decompression took {:?}", lz4_time);
    }
}
