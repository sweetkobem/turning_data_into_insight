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

    source = 'medalion.db.brz_patients'
    destination = 'medalion.db.slv_patients'

    # Initial Spark Iceberg
    spark = gnrl.connect_to_spark_iceberg(
        airflow_connection['spark_connect'])

    spark_df = spark.sql(f"""
        SELECT
            {clnr.clean_string('id')} AS patient_id,
            ({clnr.capitalized_case('FIRST')} || {clnr.capitalized_case('LAST')}) AS patient_fullname,
            BIRTHDATE AS birth_date,
            DEATHDATE AS death_date,
            {clnr.marital_status('MARITAL')} AS marital_status,
            {clnr.clean_string('RACE')} AS race,
            {clnr.clean_string('ETHNICITY')} AS ethnicity,
            {clnr.gender('GENDER')} AS gender,
            {clnr.clean_string('BIRTHPLACE')} AS birth_place,
            {clnr.clean_string('ADDRESS')} AS address,
            {clnr.clean_string('CITY')} AS city,
            {clnr.clean_string('STATE')} AS state,
            {clnr.clean_string('COUNTY')} AS county,
            {clnr.clean_string('ZIP')} AS zip,
            partition_date
        FROM {source}
        WHERE partition_date = '{execution_date}'
        """)

    # Checking data
    schema = pa.DataFrameSchema({
        "patient_id": pa.Column(
            str, regex=gnrl.regex_by_type('guid'),
            nullable=False),
        "patient_fullname": pa.Column(
            str, regex=gnrl.regex_by_type('alphabet'),
            nullable=False),
        "birth_date": pa.Column(
            str, regex=gnrl.regex_by_type('date'),
            nullable=False),
        "death_date": pa.Column(
            str, regex=gnrl.regex_by_type('date'),
            nullable=True),
        "ethnicity": pa.Column(
            str, regex=gnrl.regex_by_type('alphabet'),
            nullable=True),
        "gender": pa.Column(
            str, regex=gnrl.regex_by_type('alphabet'),
            nullable=True),
        "birth_place": pa.Column(
            str, regex=gnrl.regex_by_type('alphabet'),
            nullable=True),
        "address": pa.Column(
            str, nullable=True),
        "city": pa.Column(
            str, nullable=False),
        "state": pa.Column(
            str, nullable=False),
        "county": pa.Column(
            str, nullable=False),
        "zip": pa.Column(
            str, nullable=True),
        "partition_date": pa.Column(
            str, regex=gnrl.regex_by_type('date'),
            nullable=False),
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
