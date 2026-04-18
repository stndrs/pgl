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

# Add init script to create cleartext auth user
mkdir -p /docker-entrypoint-initdb.d
cat > /docker-entrypoint-initdb.d/01-cleartext-user.sql <<'SQL'
CREATE USER cleartext_user WITH PASSWORD 'cleartext_pass';
GRANT ALL PRIVILEGES ON DATABASE gleam_pgl_test TO cleartext_user;
SQL

# Add pg_hba entry for cleartext user (will be appended after default entries)
# We use a custom pg_hba.conf by appending to the data dir after init
cat > /docker-entrypoint-initdb.d/02-cleartext-hba.sh <<'SCRIPT'
#!/bin/bash
# Prepend a password auth rule for cleartext_user before the default scram rules
sed -i '/^host.*all.*all.*scram-sha-256/i host all cleartext_user all password' "$PGDATA/pg_hba.conf"
SCRIPT
chmod +x /docker-entrypoint-initdb.d/02-cleartext-hba.sh

# Start PostgreSQL with SSL enabled
exec docker-entrypoint.sh postgres \
  -c log_statement=all \
  -c ssl=on \
  -c ssl_cert_file=/var/lib/postgresql/server.crt \
  -c ssl_key_file=/var/lib/postgresql/server.key
