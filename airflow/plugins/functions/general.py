import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import DecimalType, TimestampType, DateType


def get_filtered_column(key_destination, columns):
    return list(
        map(lambda x: f"COALESCE(CAST({x} AS STRING), '')",
            filter(lambda x: x not in [key_destination, 'start_date', 'end_date', 'is_active'],
                   columns))
    )


def encoded_column_by_sql(filtered_column):
    return f"MD5({'''||'::'||'''.join(filtered_column)})"


def schema_dict_to_sql(schema, exclude_keys=[]):
    if exclude_keys:
        schema = {k: v for k, v in schema.items() if k not in exclude_keys}

    sql = [f"{k} {v}" for k, v in schema.items()]

    return ",".join(sql)


def is_table_exists(conn, table):
    table_exists = conn.execute(
        f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table}'
        """).fetchone()[0]

    if table_exists > 0:
        return True
    else:
        return False


def get_last_date(conn, table):
    return conn.execute(
        f"""
        SELECT
            MAX(last_date)::VARCHAR AS last_date
        FROM (
        SELECT MAX(start_date) AS last_date
        FROM {table}
        WHERE is_active IS TRUE
        UNION ALL
        SELECT MAX(end_date) AS last_date
        FROM {table}
        WHERE is_active IS TRUE
        )""").fetchone()[0]


def get_total_data_in_execution_date(conn, table, column, execution_date):
    return conn.execute(
        f"""
        SELECT
            COUNT(1) AS total
        FROM {table}
        WHERE {column}::DATE = '{execution_date}'
        """).fetchone()[0]


def dim_table_get_changes_data(
        conn, execution_date, table, key_destination, key_source, md5_column):
    return conn.execute(f"""
        SELECT
            data.*,
            '{execution_date}' AS start_date,
            NULL AS end_date,
            TRUE AS is_active,
            source.{key_destination}
        FROM (
            SELECT
                *, {md5_column}
            FROM temp_{table}
        ) data
        LEFT JOIN (
            SELECT
                *, {md5_column}
            FROM {table}
            WHERE is_active IS TRUE
        ) source
        ON (data.{key_source}=source.{key_source}
            AND source.encoded=data.encoded)
        WHERE source.encoded IS NULL
        """).fetchdf()


def dim_table_store_data(
        conn, execution_date, table, key_destination, columns, data):
    return conn.execute(f"""
            UPDATE {table}
            SET end_date = '{execution_date}', is_active = FALSE
            WHERE {key_destination} IN (
                SELECT {key_destination} FROM data
                WHERE {key_destination} IS NOT NULL
            )
            AND is_active = TRUE;

            INSERT INTO {table} ({','.join(columns)})
            SELECT {','.join(columns)}
            FROM data WHERE {key_destination} IS NULL;
            """)


def fact_table_store_data(
        conn, table, columns, data):
    return conn.execute(f"""
            INSERT INTO {table} ({','.join(columns)})
            SELECT {','.join(columns)} FROM data;
            """)


def duckdb_to_minio_server(airflow_connection, conn):
    extra = json.loads(airflow_connection['extra'])

    conn.execute("LOAD httpfs")
    conn.execute(f"SET s3_region='{extra['s3_region']}'")
    conn.execute(f"SET s3_endpoint='{airflow_connection['host']}'")
    conn.execute(f"SET s3_access_key_id='{extra['s3_access_key_id']}'")
    conn.execute(f"SET s3_secret_access_key='{extra['s3_secret_access_key']}'")
    conn.execute("SET s3_url_style='path'")
    conn.execute(f"SET s3_use_ssl={extra['s3_use_ssl']}")


def connect_to_spark_hudi(airflow_connection):
    extra = json.loads(airflow_connection['extra'])

    spark = SparkSession.builder \
        .appName("Connect to Data Lakehouse") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", airflow_connection['host']) \
        .config("spark.hadoop.fs.s3a.access.key", extra['s3_access_key_id']) \
        .config("spark.hadoop.fs.s3a.secret.key", extra['s3_secret_access_key']) \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", extra['s3_use_ssl']) \
        .config("spark.hadoop.fs.s3a.endpoint.region", extra['s3_region']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.1,org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

    return spark


def connect_to_spark_iceberg(airflow_connection):
    extra = json.loads(airflow_connection['extra'])

    spark = SparkSession.builder \
        .appName("Connect to Spark Iceberg") \
        .remote(airflow_connection['host']) \
        .config("spark.executor.cores", extra['spark.executor.cores']) \
        .config("spark.cores.max", extra['spark.cores.max']) \
        .config("spark.driver.memory", extra['spark.driver.memory']) \
        .config("spark.executor.memory", extra['spark.executor.memory']) \
        .getOrCreate()

    return spark


def regex_by_type(type):
    if type == 'guid':
        regex = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"

    elif type == 'alphabet':
        regex = r"^[A-Za-z\s\-']+$"

    elif type == 'date':
        regex = r"^(0[1-9]|[12][0-9]|3[01])-(0[1-9]|1[0-2])-\d{4}$"

    elif type == 'datetime':
        regex = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"

    elif type == 'alphanumeric':
        regex = r"^[A-Za-z0-9\s\-']+$"

    return regex


def sanitize_for_hudi(df):
    # 1. Rename columns to lowercase and replace special chars
    for old_name in df.columns:
        new_name = old_name.lower().replace(" ", "_").replace("-", "_")
        if new_name != old_name:
            df = df.withColumnRenamed(old_name, new_name)

    # 2. Convert TimestampType columns to epoch (long) or string
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampType):
            df = df.withColumn(field.name, unix_timestamp(field.name).cast("long"))

        if isinstance(field.dataType, DateType):
            df = df.withColumn(field.name, df[field.name].cast("string"))

        # Convert DecimalType columns to double (or string)
        if isinstance(field.dataType, DecimalType):
            df = df.withColumn(field.name, df[field.name].cast("double"))

    return df
