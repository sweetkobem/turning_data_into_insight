{{ config(
    materialized='custom_incremental_iceberg',
    partition_by='date'
) }}

{% set execution_date = var('execution_date') %}

SELECT
    TO_DATE(slv_procedures.stop_time) AS date,
    slv_procedures.encounter_id,
    slv_procedures.code,
    dim_patient.dim_patient_key,
    slv_procedures.description,
    slv_procedures.reason_code,
    slv_procedures.reason_description,
    slv_procedures.start_time,
    slv_procedures.stop_time,
    slv_procedures.base_cost_amount,
    (UNIX_TIMESTAMP(slv_procedures.stop_time) - UNIX_TIMESTAMP(slv_procedures.start_time)) / 60 AS duration_minute_num

  FROM medalion.db.slv_procedures
  LEFT JOIN medalion.db.dim_patient
    ON (slv_procedures.patient_id=dim_patient.patient_id
        AND dim_patient.start_date <= '{{ execution_date }}'
        AND (dim_patient.end_date > '{{ execution_date }}'
            OR dim_patient.is_active IS TRUE))

  WHERE TO_DATE(slv_procedures.partition_date) = '{{ execution_date }}'