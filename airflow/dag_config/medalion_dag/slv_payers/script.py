import sys
import base64
import pickle
import os
from datetime import datetime
import pandera as pa

# Add folder pludgins/functions and import
paths = os.path.dirname(os.path.abspath(__file__)).split('/')
index_path = paths.index('dag_config')
sys.path.append(f"{'/'.join(paths[0:index_path])}/plugins")
from functions import cleaner as clnr
from functions import general as gnrl


def main(execution_date, airflow_connection, airflow_variable):
    # Incremental method
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S').date()
    execution_date = execution_date.strftime('%Y-%m-%d')

    source = 'medalion.db.brz_payers'
    destination = 'medalion.db.slv_payers'

    # Initial Spark Iceberg
    spark = gnrl.connect_to_spark_iceberg(
        airflow_connection['spark_connect'])

    spark_df = spark.sql(f"""
        SELECT
            {clnr.clean_string('id')} AS payer_id,
            NAME AS payer_name,
            {clnr.clean_string('ADDRESS')} AS address,
            {clnr.clean_string('CITY')} AS city,
            {clnr.clean_string('ZIP')} AS zip,
            partition_date
        FROM {source}
        WHERE partition_date = '{execution_date}'
        """)

    schema = pa.DataFrameSchema({
        "payer_id": pa.Column(
            str, regex=gnrl.regex_by_type('guid'),
            nullable=False),
        "payer_name": pa.Column(
            str, regex=gnrl.regex_by_type('alphabet'),
            nullable=False),
        "address": pa.Column(
            str, nullable=True),
        "city": pa.Column(
            str, nullable=True),
        "zip": pa.Column(
            str, nullable=True),
    })

    try:
        schema.validate(spark_df.toPandas(), lazy=True)
    except pa.errors.SchemaError as e:
        print(f"Validation failed:\n{e}")

    # Saved data overwrite to Iceberg table
    spark_df.write.format("iceberg") \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy("partition_date") \
        .saveAsTable(destination)


if __name__ == '__main__':
    execution_date = sys.argv[1].replace('T', ' ')
    airflow_connection = pickle.loads(base64.b64decode(sys.argv[2]))
    airflow_variable = pickle.loads(base64.b64decode(sys.argv[3]))

    main(execution_date, airflow_connection, airflow_variable)
