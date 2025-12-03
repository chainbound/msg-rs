//! Utilies for [`std::process::Command`].

use std::{io, process};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("empty commmand provided")]
    Empty,
    #[error("io error")]
    Io(#[from] io::Error),
    #[error("non-zero exit status")]
    NonZero(Output),
}

#[derive(Debug, Clone)]
pub struct Output {
    pub status: process::ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

impl From<process::Output> for Output {
    fn from(value: process::Output) -> Self {
        Self {
            status: value.status,
            stdout: String::from_utf8_lossy(&value.stdout).to_string(),
            stderr: String::from_utf8_lossy(&value.stderr).to_string(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Runner;

impl Runner {
    /// Runs the command provided as strings, separating args with whitespaces.
    pub fn by_str(cmd: &str) -> Result<Output> {
        let mut iter = cmd.split_ascii_whitespace();
        let cmd = iter.next().ok_or(Error::Empty)?;
        let mut cmd = process::Command::new(cmd);
        cmd.args(iter).stderr(process::Stdio::piped()).stdout(process::Stdio::piped());

        tracing::debug!(?cmd, "running command");

        let output: Output = cmd.spawn()?.wait_with_output()?.into();

        if !output.status.success() {
            tracing::debug!(?output.stderr, ?output.status, ?cmd, "command returned non-zero status");
            return Err(Error::NonZero(output));
        }

        Ok(output)
    }
}
