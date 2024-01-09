use std::{net::IpAddr, process::Command};

use crate::protocol::Protocol;

/// Pipe represents a dummynet pipe.
pub struct Pipe {
    /// The ID of the pipe.
    pub id: usize,
    /// Optional bandwidth cap in Kbps.
    pub bandwidth: Option<u64>,
    /// Optional propagation delay in ms.
    pub delay: Option<u64>,
    /// Optional packet loss rate in percent.
    pub plr: Option<f64>,
}

impl Pipe {
    /// Creates a new pipe with the given ID. The ID must be unique.
    pub fn new(id: usize) -> Self {
        Self {
            id,
            bandwidth: None,
            delay: None,
            plr: None,
        }
    }

    /// Set the bandwidth cap of the pipe in Kbps.
    pub fn bandwidth(mut self, bandwidth: u64) -> Self {
        self.bandwidth = Some(bandwidth);
        self
    }

    /// Set the propagation delay of the pipe in ms.
    pub fn delay(mut self, delay: u64) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Set the packet loss rate of the pipe in percent.
    pub fn plr(mut self, plr: f64) -> Self {
        self.plr = Some(plr);
        self
    }

    pub fn id(&self) -> usize {
        self.id
    }

    /// Builds the command to create the pipe.
    pub fn build(&self) -> Command {
        let mut cmd = Command::new("sudo");

        cmd.arg("dnctl")
            .arg("pipe")
            .arg(self.id.to_string())
            .arg("config");

        if let Some(bandwidth) = self.bandwidth {
            let bw = format!("{}Kbit/s", bandwidth);

            cmd.args(["bw", &bw]);
        }

        if let Some(delay) = self.delay {
            cmd.args(["delay", &delay.to_string()]);
        }

        if let Some(plr) = self.plr {
            cmd.args(["plr", &plr.to_string()]);
        }

        cmd
    }

    /// Builds the command to destroy the pipe.
    pub fn destroy(self) -> Command {
        let mut cmd = Command::new("sudo");
        cmd.arg("dnctl")
            .arg("pipe")
            .arg("delete")
            .arg(self.id.to_string());

        cmd
    }
}

/// A wrapper around the `pfctl` command tailored to enable dummynet simulations.
pub struct PacketFilter {
    /// The name of the PF anchor to use.
    anchor: String,
    /// The supported protocols.
    protocols: Vec<Protocol>,

    /// The ID of the pipe.
    pipe_id: Option<usize>,
    /// The target endpoint of the pipe.
    endpoint: Option<IpAddr>,
}

impl Default for PacketFilter {
    fn default() -> Self {
        Self {
            anchor: "msg-sim".to_string(),
            protocols: vec![Protocol::TCP, Protocol::UDP, Protocol::ICMP],
            pipe_id: None,
            endpoint: None,
        }
    }
}

impl PacketFilter {
    /// Set the dummynet pipe ID to target.
    pub fn pipe_id(mut self, id: usize) -> Self {
        self.pipe_id = Some(id);
        self
    }

    /// Set the target endpoint for the pipe.
    pub fn endpoint(mut self, addr: IpAddr) -> Self {
        self.endpoint = Some(addr);
        self
    }

    /// Set the supported protocols.
    pub fn protocols(mut self, protocols: Vec<Protocol>) -> Self {
        self.protocols = protocols;
        self
    }

    pub fn anchor(mut self, anchor: String) -> Self {
        self.anchor = anchor;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd_to_string(cmd: &Command) -> String {
        let mut cmd_str = format!("{}", cmd.get_program().to_string_lossy());
        for arg in cmd.get_args() {
            cmd_str.push(' ');
            cmd_str.push_str(&arg.to_string_lossy());
        }

        cmd_str
    }

    #[test]
    fn test_pipe_build_cmd() {
        let cmd = Pipe::new(1).bandwidth(10).delay(100).plr(0.1).build();
        let cmd_str = cmd_to_string(&cmd);

        assert_eq!(
            cmd_str,
            "sudo dnctl pipe 1 config bw 10Kbit/s delay 100 plr 0.1"
        );

        let cmd = Pipe::new(2).delay(1000).plr(10.0).build();
        let cmd_str = cmd_to_string(&cmd);

        assert_eq!(cmd_str, "sudo dnctl pipe 2 config delay 1000 plr 10");

        let cmd = Pipe::new(3).build();
        let cmd_str = cmd_to_string(&cmd);

        assert_eq!(cmd_str, "sudo dnctl pipe 3 config");
    }

    #[test]
    fn test_pipe_destroy_cmd() {
        let pipe = Pipe::new(3);
        let cmd = pipe.destroy();
        let cmd_str = cmd_to_string(&cmd);

        assert_eq!(cmd_str, "sudo dnctl pipe delete 3")
    }
}
