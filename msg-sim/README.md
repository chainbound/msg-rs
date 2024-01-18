# `msg-sim`

## Overview
This crate provides functionality to simulate real-world network conditions
locally to and from a specific endpoint for testing and benchmarking purposes.
It only works on MacOS and Linux.

## Implementation

### MacOS
On MacOS, we use a combination of the `pfctl` and `dnctl` tools.
[`pfctl`](https://man.freebsd.org/cgi/man.cgi?query=pfctl&apropos=0&sektion=8&manpath=FreeBSD+14.0-RELEASE+and+Ports&arch=default&format=html) is a tool to manage the packet filter device. [`dnctl`](https://man.freebsd.org/cgi/man.cgi?query=dnctl&sektion=8&format=html) can manage
the [dummynet](http://info.iet.unipi.it/~luigi/papers/20100304-ccr.pdf) traffic shaper.

The general flow is as follows:

* Create a dummynet pipe with `dnctl` and configure it with `bw`, `delay`, `plr`

Example:
```bash
sudo dnctl pipe 1 config bw 10Kbit/s delay 50 plr 0.1
```

* Create a loopback alias with `ifconfig` to simulate a different endpoint and
set the MTU to the usual value (1500)

Example:
```bash
sudo ifconfig lo0 alias 127.0.0.3 up
sudo ifconfig lo0 mtu 1500
```

* Use `pfctl` to create a rule to match traffic and send it through the pipe

Example:
```bash
# Create an anchor (a named container for rules, close to a namespace)
(cat /etc/pf.conf && echo "dummynet-anchor \"msg-sim\"" && \
echo "anchor \"msg-sim\"") | sudo pfctl -f -

# Create a rule to match traffic from any to the alias and send it through the pipe
echo 'dummynet in from any to 127.0.0.3 pipe 1' | sudo pfctl -a msg-sim -f -

# Enable the packet filter
sudo pfctl -E
```

* Remove the rules and the pipe
```bash
# Apply the default configuration
sudo pfctl -f /etc/pf.conf
# Disable the packet filter
sudo pfctl -d
# Remove the alias & reset the MTU
sudo ifconfig lo0 -alias 127.0.0.3
sudo ifconfig lo0 mtu 16384
# Remove the dummynet pipes
sudo dnctl pipe delete 1
```

### Questions
- Do we need to create 2 pipes to simulate a bidirectional link? MAN page seems to say so.

### Linux
On Linux, we use dummy interfaces and `tc` with `netem` to simulate and shape traffic.
