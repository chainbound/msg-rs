# `msg-sim`

## Overview
This crate provides functionality to simulate real-world network conditions over an interface (e.g. `lo0`) for testing and benchmarking purposes.
It only works on MacOS and Linux.

## Implementation

### MacOS
* https://gist.github.com/tellyworth/2ce28add99fe743c702c090c8144355e

* `dnctl` for creating a dummynet pipe

Example:
```bash
dnctl pipe 1 config bw 10Kbit/s delay 300 plr 0.1 noerror`
```

* `pfctl` for creating a rule to match traffic and send it through the pipe

Example:
```bash
echo "dummynet out proto tcp from any to any pipe 1" | sudo pfctl -f -
pfctl -e
```
