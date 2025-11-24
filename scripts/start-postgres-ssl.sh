#!/bin/bash
set -e

# Generate self-signed certificate
openssl req -new -x509 -days 365 -nodes -text \
  -out /var/lib/postgresql/server.crt \
  -keyout /var/lib/postgresql/server.key \
  -subj '/CN=localhost'

# Set ownership and permissions
chown postgres:postgres /var/lib/postgresql/server.*
chmod 600 /var/lib/postgresql/server.key
chmod 644 /var/lib/postgresql/server.crt

# Start PostgreSQL with SSL enabled
exec docker-entrypoint.sh postgres \
  -c ssl=on \
  -c ssl_cert_file=/var/lib/postgresql/server.crt \
  -c ssl_key_file=/var/lib/postgresql/server.key
