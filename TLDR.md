# TLDR
```
# generate certificates:
docker-compuse up chainsmith

# start postgres:
docker-compuse up -d postgres

# Generate config (on a Mac):
echo "---

config/tls/int_server/certs/ca-chain-bundle.cert.pem config/tls/int_client/certs/postgres.pem config/tls/int_client/private/postgres.key.pem

client_cert_path: '$PWD/config/tls/int_client/certs/postgres.pem'
client_key_path: '$PWD/config/tls/int_client/private/postgres.key.pem'
server_ca_path: '$PWD/config/tls/int_server/certs/ca-chain-bundle.cert.pem'
host: 127.0.0.1
dbname: postgres
user: postgres" > ~/Library/Application Support/rs.postgres-ssl/default-config.yml

# Run
cargo run
