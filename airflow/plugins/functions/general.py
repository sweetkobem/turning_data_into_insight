import json
from pyspark.sql import SparkSession


def get_filtered_column(key_destination, columns):
    return list(
        map(lambda x: f"COALESCE({x}::TEXT, '')",
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
