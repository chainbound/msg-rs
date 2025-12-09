#!/usr/bin/env bash
set -e
set -x
trap 'echo "ERROR at line $LINENO: $BASH_COMMAND"' ERR

##############################################
# 1. Create namespaces: ns1, ns2, ns3, nshub
##############################################
echo "[+] Creating namespaces"
sudo ip netns add ns1 || true
sudo ip netns add ns2 || true
sudo ip netns add ns3 || true
sudo ip netns add nshub || true

##############################################
# 2. Create hub bridge inside nshub namespace
##############################################
echo "[+] Creating bridge br0 inside nshub"
sudo ip netns exec nshub ip link add br0 type bridge || true
sudo ip netns exec nshub ip link set br0 up
sudo ip netns exec nshub ip link set lo up

##############################################
# Helper: create a veth pair to each namespace
# host side → moved to nshub
##############################################
connect_to_hub() {
  ns=$1
  echo "[+] Creating link for $ns"

  # veth pair:
  #   veth-$ns-h   ← will be moved into nshub
  #   $ns-hub      ← stays in target namespace
  sudo ip link add veth-$ns-h type veth peer name ${ns}-hub

  # Move peer into ns
  sudo ip link set ${ns}-hub netns $ns

  # Move other end into nshub
  sudo ip link set veth-$ns-h netns nshub

  # Bring up ns side
  sudo ip netns exec $ns ip link set lo up
  sudo ip netns exec $ns ip link set ${ns}-hub up

  # Bring up hub side + attach to bridge
  sudo ip netns exec nshub ip link set veth-$ns-h master br0
  sudo ip netns exec nshub ip link set veth-$ns-h up
}

connect_to_hub ns1
connect_to_hub ns2
connect_to_hub ns3

##############################################
# 3. Assign IPs inside each namespace
##############################################
echo "[+] Assigning IP addresses"
sudo ip netns exec ns1 ip addr add 10.0.0.1/24 dev ns1-hub
sudo ip netns exec ns2 ip addr add 10.0.0.2/24 dev ns2-hub
sudo ip netns exec ns3 ip addr add 10.0.0.3/24 dev ns3-hub

##############################################
# 4. Apply impairment ONLY for ns1 → ns2
##############################################
echo "[+] Applying impairment ONLY for ns1 -> ns2"

# Root qdisc
sudo ip netns exec ns1 tc qdisc add dev ns1-hub root handle 1: prio

# Netem qdisc for band 3
sudo ip netns exec ns1 tc qdisc add dev ns1-hub parent 1:3 handle 30: netem loss 40%

# Filter: match DESTINATION 10.0.0.2 only
sudo ip netns exec ns1 tc filter add dev ns1-hub protocol ip parent 1: prio 3 u32 \
  match ip dst 10.0.0.2/32 flowid 1:3

##############################################
# 5. Instructions
##############################################
echo ""
echo "[+] Topology ready!"
echo "Hub is inside namespace: nshub"
echo ""
echo "IMPAIRMENT ENABLED:"
echo "  ns1 -> ns2 has 40% loss"
echo "  ns1 -> ns3 is normal"
echo "  ns2 -> ns1 is normal"
echo ""
echo "TEST CONNECTIVITY:"
echo "  sudo ip netns exec ns1 ping 10.0.0.2   # impaired"
echo "  sudo ip netns exec ns1 ping 10.0.0.3   # normal"
echo ""
echo "TO REMOVE IMPAIRMENTS:"
echo "  sudo ip netns exec ns1 tc qdisc del dev ns1-hub root"
echo ""
echo "Done!"
