use std::{io, process::ExitStatus};

/// Assert that the given status is successful, otherwise return an error with the given message.
/// The type of the error will be `io::ErrorKind::Other`.
pub fn assert_status<E>(status: ExitStatus, error: E) -> io::Result<()>
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    if !status.success() {
        return Err(io::Error::new(io::ErrorKind::Other, error));
    }

    Ok(())
}
