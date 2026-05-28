#!/bin/bash
set -e

# Generate sshd host keys on first start (no-op if they already exist).
ssh-keygen -A

# Install the public key as root's authorized_keys.
if [ -f /keys/id_ed25519.pub ]; then
  install -d -m 700 -o root -g root /root/.ssh
  install -m 600 -o root -g root /keys/id_ed25519.pub /root/.ssh/authorized_keys
else
  echo "WARN: /keys/id_ed25519.pub not found; SSH login will fail." >&2
fi

# Persist CASSANDRA_* + heap env so SSH-triggered restarts can recover them.
# CassandraKiller runs `sudo /etc/init.d/cassandra start`, and sudo strips the
# environment by default without this, the upstream entrypoint would re-run
# with empty env, rewriting cassandra.yaml so the seed list collapses to the
# node's own IP and the heap reverts to defaults that OOM-kill the JVM.
declare -x | grep -E ' (CASSANDRA_[A-Z_]+|MAX_HEAP_SIZE|HEAP_NEWSIZE)=' \
    > /etc/cassandra/cluster.env || true

# Background Cassandra via the init script (writes the PID file the killer reads).
/etc/init.d/cassandra start

# sshd as PID 1 so the container survives `pkill cassandra`.
mkdir -p /run/sshd
exec /usr/sbin/sshd -D -e
