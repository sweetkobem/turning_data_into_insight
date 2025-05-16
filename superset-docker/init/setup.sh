#!/bin/bash
set -e

echo "Starting Superset setup..."

echo "Sourcing environment variables from .env"
source /app/.env

echo "Waiting for PostgreSQL..."
sleep 10

# Set environment variables
export SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

# Upgrade the database
superset db upgrade

# Check if user exists
if ! superset fab list-users | grep -q "$ADMIN_USERNAME"; then
  echo "Creating Superset admin user '${ADMIN_USERNAME}'..."
  superset fab create-admin \
    --username "${ADMIN_USERNAME}" \
    --firstname "Superset" \
    --lastname "Admin" \
    --email "${ADMIN_EMAIL}" \
    --password "${ADMIN_PASSWORD}"
else
  echo "Superset user '${ADMIN_USERNAME}' already exists. Skipping creation."
fi

# Initialize Superset
superset init

# Start the Superset server
exec superset run -h 0.0.0.0 -p 8088