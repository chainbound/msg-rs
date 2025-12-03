# `msg-sim`

todo:

- [ ] run on namespace
- [ ] apply netem on localhost or link

## Overview

## Implementation

### Linux

On Linux, we leverage network namespaces to simulate different networking
conditions, leveraging the `tc` and `netem` command to shape traffic.

On each namespace, we create a veth pair, with one end in the default namespace,
and then we configure the veth devices as needed.

The general flow is as follows:

- Create a network namespace with `ip netns add`
- Add a veth pair to the namespace with `ip link add`
- Set the veth pair up with `ip link set`
- Set the IP address of the veth pair in the namespace with `ip netns exec`
- Set the network emulation parameters with `tc qdisc add dev` both in the host and
  the namespaced environment

Example:

```bash
# create namespace ns1
sudo ip netns add ns1
# create veth devices linked together
sudo ip link add veth-host type veth peer name veth-ns1
# move veth-ns1 device to ns1 namespace
sudo ip link set veth-ns1 netns ns1

# associate ip addr to veth-host device and spin it up
sudo ip addr add 192.168.1.2/24 dev veth-host
sudo ip link set veth-host up

# same but from ns1 namespace
sudo ip netns exec ns1 ip addr add 192.168.1.1/24 dev veth-ns1
sudo ip netns exec ns1 ip link set veth-ns1 up

# add latency etc to veth-ns1 from ns1 namespace
sudo ip netns exec ns1 tc qdisc add dev veth-ns1 root netem delay 3000ms loss 50%

# this should be slow
ping 192.168.1.1
```

#### How to run tests

Given that the tests require root privileges to modify the networking stack,
you can run them with the following command:

```bash
sudo HOME=$HOME $(which cargo) test # add your arguments here
```

We need to provide the `$HOME` environment variable to `sudo` to ensure that
it can find the Rust toolchain, and then we also need to provide the path of `cargo`.
