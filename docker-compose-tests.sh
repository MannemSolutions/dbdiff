#!/bin/bash
set -e

docker-compose down --remove-orphans || echo new or partial install
docker-compose up chainsmith
docker-compose up -d postgres
sleep 5
cargo run
