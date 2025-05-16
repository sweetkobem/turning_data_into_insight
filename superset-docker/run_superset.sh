#!/bin/bash
set -e

# Load .env file variables into environment
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Define the path to superset_config.py
SUPSET_CONFIG_PATH="./superset/superset_config.py"

# Generate a random SECRET_KEY (42 characters long)
SECRET_KEY=$(openssl rand -base64 42)

# Check if the superset_config.py file exists, create it if not
if [ ! -f "$SUPSET_CONFIG_PATH" ]; then
    echo "superset_config.py does not exist, creating it."
    touch "$SUPSET_CONFIG_PATH"
fi

# Write the generated SECRET_KEY to superset_config.py
echo "Setting the SECRET_KEY in superset_config.py"
cat > "$SUPSET_CONFIG_PATH" <<EOL
# superset_config.py

# Set a strong SECRET_KEY for encryption
SECRET_KEY = '$SECRET_KEY'
RATELIMIT_STORAGE_URI = "redis://redis:6379/0"
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}"
)
EOL

echo "SECRET_KEY has been set in superset_config.py."

# Optionally, restart Docker to apply changes
docker-compose down
docker-compose up --build -d

echo "Superset container restarted."
