import os
from dotenv import load_dotenv

# Load from .env file
load_dotenv()

# Access environment variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_WAREHOUSE = os.getenv("MINIO_WAREHOUSE")
MINIO_REGION = os.getenv("MINIO_REGION")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

# Output path for the config file
output_path = "config/spark-defaults.conf"

# Make sure output directory exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Spark configuration
config = f"""\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.medalion=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.medalion.type=hadoop
spark.sql.catalog.medalion.warehouse={MINIO_WAREHOUSE}

# MinIO settings
spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT}
spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY}
spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY}
spark.hadoop.fs.s3a.endpoint.region={MINIO_REGION}
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.connection.ssl.enabled=false
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
"""

# Write the config file
with open(output_path, "w") as f:
    f.write(config)
