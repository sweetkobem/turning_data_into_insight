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

    source = 'medalion.db.brz_encounters'
    destination = 'medalion.db.slv_encounters'

    # Initial Spark Iceberg
    spark = gnrl.connect_to_spark_iceberg(
        airflow_connection['spark_connect'])

    spark_df = spark.sql(f"""
        SELECT
            {clnr.clean_string('id')} AS encounter_id,
            START AS start_time,
            STOP AS stop_time,
            {clnr.clean_string('PATIENT')} AS patient_id,
            {clnr.clean_string('PAYER')} AS payer_id,
            {clnr.upper_case('ENCOUNTERCLASS')} AS encounter_class,
            CODE AS code,
            {clnr.clean_string('DESCRIPTION')} AS description,
            CAST(BASE_ENCOUNTER_COST AS DOUBLE) AS base_cost_amount,
            CAST(TOTAL_CLAIM_COST AS DOUBLE) AS claim_amount,
            CAST(PAYER_COVERAGE AS DOUBLE) AS coverage_amount,
            REASONCODE AS reason_code,
            {clnr.clean_string('REASONDESCRIPTION')} AS reason_description,
            partition_date
        FROM {source}
        WHERE partition_date = '{execution_date}'
        """)

    # Checking data
    schema = pa.DataFrameSchema({
        "encounter_id": pa.Column(
            str, regex=gnrl.regex_by_type('guid'),
            nullable=False),
        "start_time": pa.Column(
            str, regex=gnrl.regex_by_type('datetime'),
            nullable=False),
        "stop_time": pa.Column(
            str, regex=gnrl.regex_by_type('datetime'),
            nullable=False),
        "patient_id": pa.Column(
            str, regex=gnrl.regex_by_type('guid'),
            nullable=False),
        "payer_id": pa.Column(
            str, regex=gnrl.regex_by_type('guid'),
            nullable=False),
        "encounter_class": pa.Column(
            str, regex=gnrl.regex_by_type('alphabet'),
            nullable=True),
        "code": pa.Column(
            str, regex=gnrl.regex_by_type('alphanumeric'),
            nullable=True),
        "description": pa.Column(
            str, nullable=True),
        "base_cost_amount": pa.Column(
            float, nullable=False),
        "claim_amount": pa.Column(
            float, nullable=False),
        "coverage_amount": pa.Column(
            float, nullable=False),
        "reason_code": pa.Column(
            str, regex=gnrl.regex_by_type('alphanumeric'),
            nullable=True),
        "reason_description": pa.Column(
            str, nullable=True)
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
