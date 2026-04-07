#!/bin/bash
# setup-volumes.sh - Automate Docker volume and data directory setup

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo " Setting up Docker volumes..."

# Create volume directories
mkdir -p ./volumes/postgres_data
mkdir -p ./volumes/prometheus_data
mkdir -p ./volumes/grafana_data

echo "✓ Directories created"

# Set permissions for PostgreSQL (UID 999)
echo " Setting PostgreSQL permissions..."
sudo chown -R 999:999 ./volumes/postgres_data
sudo chmod -R 700 ./volumes/postgres_data

# Set permissions for Prometheus (UID 65534)
echo " Setting Prometheus permissions..."
sudo chown -R 65534:65534 ./volumes/prometheus_data
sudo chmod -R 755 ./volumes/prometheus_data

# Prometheus requires write access to queries.active and TSDB files
sudo chmod 755 ./volumes/prometheus_data

# Set permissions for Grafana (UID 472)
echo " Setting Grafana permissions..."
sudo chown -R 472:472 ./volumes/grafana_data
sudo chmod -R 755 ./volumes/grafana_data

echo ""
echo " Volume setup complete!"
echo ""
echo "Verify with:"
echo "  ls -la ./volumes/"
echo ""
echo "Next steps:"
echo "  1. docker compose down -v   (optional, to clean old state)"
echo "  2. docker compose up --build"
