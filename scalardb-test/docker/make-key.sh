#!/bin/bash
set -e

KEY_DIR="$(cd "$(dirname "$0")" && pwd)/ssh"
mkdir -p "$KEY_DIR"
cd "$KEY_DIR"

if [ -f id_ed25519 ]; then
  echo "Key already exists at $KEY_DIR/id_ed25519. Skipping."
  exit 0
fi

ssh-keygen -t ed25519 -N "" -C "scalardb-test" -f id_ed25519
chmod 600 id_ed25519
chmod 644 id_ed25519.pub
echo "Generated SSH key pair at $KEY_DIR/."
