#!/bin/bash
set -euo pipefail

IMAGE="$1"

# Generate self-signed certificate for SSL
openssl req -new -x509 -days 365 -nodes -text \
  -out server.crt -keyout server.key \
  -subj "/CN=localhost"

CONTAINER=$(docker ps -q --filter "ancestor=$IMAGE")

# Enable SSL
docker cp server.crt "$CONTAINER":/var/lib/postgresql/server.crt
docker cp server.key "$CONTAINER":/var/lib/postgresql/server.key
docker exec "$CONTAINER" chown postgres:postgres /var/lib/postgresql/server.crt /var/lib/postgresql/server.key
docker exec "$CONTAINER" chmod 600 /var/lib/postgresql/server.key
docker exec "$CONTAINER" psql -U postgres -c "ALTER SYSTEM SET ssl = 'on';"
docker exec "$CONTAINER" psql -U postgres -c "ALTER SYSTEM SET ssl_cert_file = '/var/lib/postgresql/server.crt';"
docker exec "$CONTAINER" psql -U postgres -c "ALTER SYSTEM SET ssl_key_file = '/var/lib/postgresql/server.key';"
docker exec "$CONTAINER" psql -U postgres -c "SELECT pg_reload_conf();"

# Create test users
docker exec "$CONTAINER" psql -U postgres -c "CREATE USER cleartext_user WITH PASSWORD 'cleartext_pass';"
docker exec "$CONTAINER" psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE pgl_test TO cleartext_user;"
docker exec "$CONTAINER" psql -U postgres -c "CREATE USER trust_user;"
docker exec "$CONTAINER" psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE pgl_test TO trust_user;"
docker exec "$CONTAINER" psql -U postgres -c "SET password_encryption = 'md5'; CREATE USER md5_user WITH PASSWORD 'md5_pass';"
docker exec "$CONTAINER" psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE pgl_test TO md5_user;"

# Configure pg_hba.conf for auth methods
HBA_FILE=$(docker exec "$CONTAINER" psql -U postgres -tAc "SHOW hba_file;")
docker exec "$CONTAINER" bash -c "sed -i '/^host.*all.*all.*scram-sha-256/i host all cleartext_user all password' $HBA_FILE"
docker exec "$CONTAINER" bash -c "sed -i '/^host.*all.*all.*scram-sha-256/i host all trust_user all trust' $HBA_FILE"
docker exec "$CONTAINER" bash -c "sed -i '/^host.*all.*all.*scram-sha-256/i host all md5_user all md5' $HBA_FILE"
docker exec "$CONTAINER" psql -U postgres -c "SELECT pg_reload_conf();"
