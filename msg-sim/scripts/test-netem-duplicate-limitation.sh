#!/bin/bash

# Reproduces Linux kernel limitation: once a netem qdisc with duplicate > 0
# exists on an interface, no additional netem qdiscs can be created.
#
# The kernel logs "netem: cannot mix duplicating netems with other netems in tree" and returns EINVAL (-22).
#
# Usage: sudo ./test-netem-duplicate-limitation.sh [--debug]
#
# Options:
#   --debug    Enable detailed debugging (strace, ftrace)

set -e

DEBUG=0
if [[ "$1" == "--debug" ]]; then
  DEBUG=1
fi

NETNS="netem-test-ns"
VETH_HOST="veth-test-host"
VETH_NS="veth-test-ns"

cleanup() {
  echo ""
  echo "Cleaning up..."

  # Always reset ftrace to safe state (in case script was interrupted)
  if [[ -d /sys/kernel/debug/tracing ]]; then
    sudo sh -c 'echo 0 > /sys/kernel/debug/tracing/tracing_on' 2>/dev/null || true
    sudo sh -c 'echo > /sys/kernel/debug/tracing/set_ftrace_filter' 2>/dev/null || true
    sudo sh -c 'echo nop > /sys/kernel/debug/tracing/current_tracer' 2>/dev/null || true
    sudo sh -c 'echo > /sys/kernel/debug/tracing/trace' 2>/dev/null || true
  fi

  # Clean up network namespace and veth pair
  sudo ip netns del "$NETNS" 2>/dev/null || true
  sudo ip link del "$VETH_HOST" 2>/dev/null || true

  echo "Cleanup complete."
}

trap cleanup EXIT

echo "=== Netem Duplicate Limitation Reproduction ==="
echo ""
echo "Kernel version: $(uname -r)"
echo "Debug mode: $([[ $DEBUG -eq 1 ]] && echo 'enabled' || echo 'disabled')"
echo ""

# Create network namespace and veth pair
echo "1. Creating network namespace and veth pair..."
sudo ip netns add "$NETNS"
sudo ip link add "$VETH_HOST" type veth peer name "$VETH_NS"
sudo ip link set "$VETH_NS" netns "$NETNS"
sudo ip link set "$VETH_HOST" up
sudo ip netns exec "$NETNS" ip link set "$VETH_NS" up
sudo ip netns exec "$NETNS" ip addr add 10.99.0.1/24 dev "$VETH_NS"

# Get interface index
IFINDEX=$(sudo ip netns exec "$NETNS" cat /sys/class/net/"$VETH_NS"/ifindex)
echo "   Interface: $VETH_NS (index $IFINDEX)"

# Create DRR root qdisc
echo ""
echo "2. Creating DRR root qdisc (handle 1:0)..."
sudo ip netns exec "$NETNS" tc qdisc add dev "$VETH_NS" root handle 1: drr

# Create two DRR classes
echo ""
echo "3. Creating DRR classes..."
sudo ip netns exec "$NETNS" tc class add dev "$VETH_NS" parent 1: classid 1:10 drr
sudo ip netns exec "$NETNS" tc class add dev "$VETH_NS" parent 1: classid 1:20 drr
echo "   Created classes 1:10 and 1:20"

# Create first netem WITH duplicate under class 1:10
echo ""
echo "4. Creating first netem WITH duplicate (handle 10:0, parent 1:10)..."
sudo ip netns exec "$NETNS" tc qdisc add dev "$VETH_NS" parent 1:10 handle 10: netem delay 10ms duplicate 1%
echo "   SUCCESS: First netem with duplicate created"

# Show current qdisc configuration
echo ""
echo "5. Current qdisc configuration:"
sudo ip netns exec "$NETNS" tc qdisc show dev "$VETH_NS"

# Enable ftrace if debug mode
if [[ $DEBUG -eq 1 ]] && [[ -d /sys/kernel/debug/tracing ]]; then
  echo ""
  echo "=== Enabling kernel tracing ==="
  sudo sh -c 'echo > /sys/kernel/debug/tracing/trace'
  sudo sh -c 'echo "netem_*" > /sys/kernel/debug/tracing/set_ftrace_filter' 2>/dev/null || true
  sudo sh -c 'echo function > /sys/kernel/debug/tracing/current_tracer'
  sudo sh -c 'echo 1 > /sys/kernel/debug/tracing/tracing_on'
fi

# Try to create second netem WITHOUT duplicate under class 1:20
echo ""
echo "6. Attempting to create second netem WITHOUT duplicate (handle 20:0, parent 1:20)..."
echo "   (This is expected to FAIL due to kernel limitation)"
echo ""

# Capture the command output
TC_CMD="tc qdisc add dev $VETH_NS parent 1:20 handle 20: netem delay 20ms"

if [[ $DEBUG -eq 1 ]]; then
  echo "=== Running with strace ==="
  echo ""
  # Run with strace to capture netlink messages
  sudo strace -e sendmsg,recvmsg -s 2000 ip netns exec "$NETNS" $TC_CMD 2>&1 || true
  echo ""

  # Show ftrace output
  if [[ -d /sys/kernel/debug/tracing ]]; then
    echo "=== Kernel ftrace output ==="
    sudo cat /sys/kernel/debug/tracing/trace | grep -v "^#" | tail -50
    echo ""
  fi
else
  # Run with verbose tc output
  echo "=== tc verbose output ==="
  sudo ip netns exec "$NETNS" tc -d $TC_CMD 2>&1 || true
  echo ""
fi

# Check dmesg for kernel messages
echo "=== Recent dmesg (netem related) ==="
sudo dmesg | grep -i netem | tail -10 || echo "(no netem messages found)"

echo ""
echo "=== Test Complete ==="
echo ""
echo "Summary: Once a netem qdisc with 'duplicate > 0' exists on an interface,"
echo "the Linux kernel prevents creating any additional netem qdiscs on that"
echo "interface, even if the new netem doesn't use duplicate."
echo ""
echo "To get more debugging info, run with: $0 --debug"
