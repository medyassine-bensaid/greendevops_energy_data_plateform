#!/bin/sh
set -e

# 1. Ensure the .ssh directory exists inside the container's isolated /app folder
mkdir -p /app/.ssh

# 2. Copy the private key from the secure read-only volume mount
if [ -f /mnt/ssh/id_rsa ]; then
    cp /mnt/ssh/id_rsa /app/.ssh/id_rsa
fi

# 3. Secure the permissions so SSH doesn't reject the key for being too exposed
chmod 700 /app/.ssh
if [ -f /app/.ssh/id_rsa ]; then
    chmod 600 /app/.ssh/id_rsa
fi

# 4. Configure automated headless SSH access for Grid5000 frontends
cat <<EOF > /app/.ssh/config
Host *.grid5000.fr
    IdentityFile /app/.ssh/id_rsa
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
EOF
chmod 600 /app/.ssh/config

# 5. PRODUCTION STANDARD: Fix ownership of work directories and mounted data volumes
mkdir -p /data/raw /data/parquet
chown -R app:app /app /data/raw /data/parquet

# 6. Securely drop privileges from root to app and start your untouched Go binary
exec su-exec app ./ingestion
