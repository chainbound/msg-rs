use std::{
    io::{self, Read},
    net::IpAddr,
    process::{Command, Stdio},
};

use crate::{assert::assert_status, protocol::Protocol};

/// Pipe represents a dummynet pipe.
pub struct Pipe {
    /// The ID of the pipe.
    pub id: u8,
    /// Optional bandwidth cap in Kbps.
    pub bandwidth: Option<u64>,
    /// Optional propagation delay in ms.
    pub delay: Option<u128>,
    /// Optional packet loss rate in percent.
    pub plr: Option<f64>,
}

impl Pipe {
    /// Creates a new pipe with the given ID. The ID must be unique.
    pub fn new(id: u8) -> Self {
        Self { id, bandwidth: None, delay: None, plr: None }
    }

    /// Set the bandwidth cap of the pipe in Kbps.
    pub fn bandwidth(mut self, bandwidth: u64) -> Self {
        self.bandwidth = Some(bandwidth);
        self
    }

    /// Set the propagation delay of the pipe in ms.
    pub fn delay(mut self, delay: u128) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Set the packet loss rate of the pipe in percent.
    pub fn plr(mut self, plr: f64) -> Self {
        self.plr = Some(plr);
        self
    }

    pub fn id(&self) -> u8 {
        self.id
    }

    fn build_cmd(&self) -> Command {
        let mut cmd = Command::new("sudo");

        cmd.arg("dnctl").arg("pipe").arg(self.id.to_string()).arg("config");

        if let Some(bandwidth) = self.bandwidth {
            let bw = format!("{bandwidth}Kbit/s");

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

    /// Builds the command to create the pipe.
    pub fn build(&self) -> io::Result<()> {
        let status = self.build_cmd().status()?;

        assert_status(status, "Failed to build pipe")?;

        Ok(())
    }

    fn destroy_cmd(&self) -> Command {
        let mut cmd = Command::new("sudo");
        cmd.arg("dnctl").arg("pipe").arg("delete").arg(self.id.to_string());

        cmd
    }

    /// Destroys the pipe.
    pub fn destroy(self) -> io::Result<()> {
        let status = self.destroy_cmd().status()?;

        assert_status(status, "Failed destroying pipe")?;

        Ok(())
    }
}

/// A wrapper around the `pfctl` command tailored to enable dummynet simulations.
pub struct PacketFilter {
    /// The pipe itself.
    pipe: Pipe,
    /// The name of the PF anchor to use.
    anchor: String,
    /// The supported protocols.
    protocols: Vec<Protocol>,
    /// The target endpoint of the pipe.
    endpoint: Option<IpAddr>,
    /// The name of the loopback interface
    loopback: String,
}

impl PacketFilter {
    /// Creates a new default packet filter from the given [`Pipe`].
    pub fn new(pipe: Pipe) -> Self {
        let id = pipe.id();
        Self {
            pipe,
            anchor: format!("msg-sim-{}", id),
            protocols: vec![Protocol::Tcp, Protocol::Udp, Protocol::Icmp],
            endpoint: None,
            loopback: get_loopback_name(),
        }
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

    /// Set the name of the anchor.
    pub fn anchor(mut self, anchor: impl Into<String>) -> Self {
        self.anchor = anchor.into();
        self
    }

    /// Enables the packet filter by executing the correct shell commands.
    pub fn enable(&self) -> io::Result<()> {
        // Build the pipe.
        self.pipe.build()?;

        self.create_loopback_alias()?;

        self.load_pf_config()?;
        self.apply_dummynet_rule()?;

        // Enable the packet filter
        let status = Command::new("sudo").args(["pfctl", "-E"]).status()?;

        assert_status(status, "Failed to enable packet filter")?;

        Ok(())
    }

    /// Destroys the packet filter by executing the correct shell commands.
    pub fn destroy(self) -> io::Result<()> {
        let status = Command::new("sudo").args(["pfctl", "-f", "/etc/pf.conf"]).status()?;

        assert_status(status, "Failed to flush packet filter")?;

        let status = Command::new("sudo").args(["pfctl", "-d"]).status()?;

        assert_status(status, "Failed to disable packet filter")?;

        // Remove the loopback alias
        let status = Command::new("sudo")
            .args(["ifconfig", &self.loopback, "-alias", &self.endpoint.unwrap().to_string()])
            .status()?;

        assert_status(status, "Failed to remove the loopback alias")?;

        // Reset the MTU of the loopback interface

        let status =
            Command::new("sudo").args(["ifconfig", &self.loopback, "mtu", "16384"]).status()?;

        assert_status(status, "Failed to reset loopback MTU back to 16384")?;

        // Destroy the pipe
        self.pipe.destroy()?;

        Ok(())
    }

    fn create_loopback_alias(&self) -> io::Result<()> {
        let status = Command::new("sudo")
            .args(["ifconfig", &self.loopback, "alias", &self.endpoint.unwrap().to_string()])
            .status()?;

        assert_status(status, "Failed to create loopback alias")?;

        let status =
            Command::new("sudo").args(["ifconfig", &self.loopback, "mtu", "1500"]).status()?;

        assert_status(status, "Failed to set loopback MTU to 1500")?;

        Ok(())
    }

    /// Loads pfctl config with anchor by executing this command:
    /// `(cat /etc/pf.conf && echo "dummynet-anchor \"msg-sim\"" &&
    /// echo "anchor \"msg-sim\"") | sudo pfctl -f -`
    fn load_pf_config(&self) -> io::Result<()> {
        let echo_cmd = format!("dummynet-anchor \"{}\"\nanchor \"{}\"", self.anchor, self.anchor);

        let mut cat = Command::new("cat").arg("/etc/pf.conf").stdout(Stdio::piped()).spawn()?;

        let cat_stdout = cat.stdout.take().unwrap();

        let mut echo = Command::new("echo").arg(echo_cmd).stdout(Stdio::piped()).spawn()?;

        let echo_stdout = echo.stdout.take().unwrap();

        let mut pfctl =
            Command::new("sudo").arg("pfctl").arg("-f").arg("-").stdin(Stdio::piped()).spawn()?;

        let pfctl_stdin = pfctl.stdin.as_mut().unwrap();
        io::copy(&mut cat_stdout.chain(echo_stdout), pfctl_stdin)?;

        let status = pfctl.wait()?;

        assert_status(status, "Failed to load pfctl config")?;

        Ok(())
    }

    /// Applies a rule to match traffic from any to the alias and sends that through the pipe
    /// by executing this command:
    /// `echo 'dummynet in from any to 127.0.0.3 pipe 1' | sudo pfctl -a msg-sim -f -`
    fn apply_dummynet_rule(&self) -> io::Result<()> {
        // Ensure endpoint and pipe ID are set
        let endpoint = self.endpoint.expect("No endpoint set");
        let pipe_id = self.pipe.id();

        let echo_command = format!("dummynet in from any to {endpoint} pipe {pipe_id}");

        // Set up the echo command
        let mut echo = Command::new("echo").arg(echo_command).stdout(Stdio::piped()).spawn()?;

        if let Some(echo_stdout) = echo.stdout.take() {
            // Set up the pfctl command
            let mut pfctl = Command::new("sudo")
                .arg("pfctl")
                .arg("-a")
                .arg(&self.anchor)
                .arg("-f")
                .arg("-")
                .stdin(echo_stdout)
                .spawn()?;

            pfctl.wait()?;
        }

        Ok(())
    }
}

/// Returns the name of the loopback interface.
///
/// ## Panics
/// Panics if no loopback interface is found.
fn get_loopback_name() -> String {
    let interfaces = pnet::datalink::interfaces();
    let loopback = interfaces.into_iter().find(|iface| iface.is_loopback());

    loopback.expect("No loopback interface").name
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
    #[ignore]
    fn test_pipe_build_cmd() {
        let cmd = Pipe::new(1).bandwidth(10).delay(100).plr(0.1).build_cmd();
        let cmd_str = cmd_to_string(&cmd);

        assert_eq!(cmd_str, "sudo dnctl pipe 1 config bw 10Kbit/s delay 100 plr 0.1");

        let cmd = Pipe::new(2).delay(1000).plr(10.0).build_cmd();
        let cmd_str = cmd_to_string(&cmd);

        assert_eq!(cmd_str, "sudo dnctl pipe 2 config delay 1000 plr 10");

        let cmd = Pipe::new(3).build_cmd();
        let cmd_str = cmd_to_string(&cmd);

        assert_eq!(cmd_str, "sudo dnctl pipe 3 config");
    }

    #[test]
    #[ignore]
    fn test_pipe_destroy_cmd() {
        let pipe = Pipe::new(3);
        let cmd = pipe.destroy_cmd();
        let cmd_str = cmd_to_string(&cmd);

        assert_eq!(cmd_str, "sudo dnctl pipe delete 3")
    }

    #[test]
    #[ignore]
    fn dummy_tests() {
        let pipe = Pipe::new(3).bandwidth(100).delay(300);

        let endpoint = "127.0.0.2".parse().unwrap();
        let pf = PacketFilter::new(pipe).endpoint(endpoint).anchor("msg-sim-test");

        pf.enable().unwrap();

        std::thread::sleep(Duration::from_secs(20));
        pf.destroy().unwrap();
    }
}
