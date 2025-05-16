#!/bin/bash

# Generate file generate_spark-defaults.conf.py based on .env file.
python3 generate_spark-defaults.conf.py

# Run/restart docker
docker-compose down
docker-compose up --build -d

echo "Spark container restarted."