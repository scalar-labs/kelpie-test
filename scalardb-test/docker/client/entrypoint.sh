#!/bin/bash
set -e

# Copy the SSH key out of the read-only mount so we can chmod it. OpenSSH
# refuses to use a key that is group/world-readable.
if [ -f /keys/id_ed25519 ]; then
  install -d -m 700 /root/.ssh
  install -m 600 /keys/id_ed25519     /root/.ssh/id_ed25519
  install -m 644 /keys/id_ed25519.pub /root/.ssh/id_ed25519.pub
  cat > /root/.ssh/config <<'EOF'
Host cassandra*
  User root
  IdentityFile /root/.ssh/id_ed25519
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
EOF
  chmod 600 /root/.ssh/config
fi

exec "$@"
