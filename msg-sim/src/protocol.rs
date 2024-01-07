#[derive(Debug, Clone, Copy)]
#[allow(clippy::upper_case_acronyms)]
pub enum Protocol {
    TCP,
    UDP,
    ICMP,
}

impl From<&str> for Protocol {
    fn from(s: &str) -> Self {
        match s {
            "tcp" => Self::TCP,
            "udp" => Self::UDP,
            "icmp" => Self::ICMP,
            _ => panic!("invalid protocol"),
        }
    }
}
