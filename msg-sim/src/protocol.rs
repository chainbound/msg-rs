#[derive(Debug, Clone, Copy)]
pub enum Protocol {
    Tcp,
    Udp,
    Icmp,
}

impl From<&str> for Protocol {
    fn from(s: &str) -> Self {
        match s {
            "tcp" => Self::Tcp,
            "udp" => Self::Udp,
            "icmp" => Self::Icmp,
            _ => panic!("invalid protocol"),
        }
    }
}
