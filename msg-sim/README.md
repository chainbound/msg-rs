# `msg-sim`

## Overview
This crate provides functionality to simulate real-world network conditions over an interface (e.g. `lo0`) for testing and benchmarking purposes.
It only works on MacOS and Linux.

## Implementation

### MacOS
On MacOS, we use a combination of the `pfctl` and `dnctl` tools.
`pfctl` is a tool to manage the packet filter device. `dnctl` can manage
the dummynet traffic shaper.

The general flow is as follows:

* Create a dummynet pipe with `dnctl` and configure it with `bw`, `delay`, `plr` and `noerror` parameters.

Example:
```bash
sudo dnctl pipe 1 config bw 10Kbit/s delay 300 plr 0.1 noerror
```

* Use `pfctl` to create a rule to match traffic and send it through the pipe

Example:
```bash
# Create an anchor (a named container for rules)
(cat /etc/pf.conf && echo "dummynet-anchor \"msg-sim\"" && \
echo "anchor \"msg-sim\"") | sudo pfctl -f -

INTERFACE="lo0"
echo 'dummynet in on $INTERFACE all pipe 1' | sudo pfctl -a msg-sim -f -

# Enable the packet filter
sudo pfctl -E
```

* `pfctl` for removing the rule
```bash
# Apply the default configuration
sudo pfctl -f /etc/pf.conf
# Disable the packet filter
sudo pfctl -d
# Remove the dummynet pipes
sudo dnctl -q flush
```
