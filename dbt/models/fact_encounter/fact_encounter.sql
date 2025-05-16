{{ config(
    materialized='custom_incremental_iceberg',
    partition_by='date'
) }}

{% set execution_date = var('execution_date') %}

SELECT
  TO_DATE(slv_encounters.stop_time) AS date,
  slv_encounters.encounter_id,
  dim_patient.dim_patient_key,
  dim_payer.dim_payer_key,
  slv_encounters.encounter_class,
  slv_encounters.code,
  slv_encounters.description,
  slv_encounters.reason_code,
  slv_encounters.reason_description,
  slv_encounters.start_time,
  slv_encounters.stop_time,
  slv_encounters.base_cost_amount,
  slv_encounters.claim_amount,
  slv_encounters.coverage_amount,
  slv_encounters.claim_amount - slv_encounters.coverage_amount AS patient_pay_amount,
  DATEDIFF(slv_encounters.stop_time, slv_encounters.start_time) AS length_of_stay_num

FROM medalion.db.slv_encounters
LEFT JOIN medalion.db.dim_patient
  ON (slv_encounters.patient_id=dim_patient.patient_id
      AND dim_patient.start_date <= '{{ execution_date }}'
      AND (dim_patient.end_date > '{{ execution_date }}'
          OR dim_patient.is_active IS TRUE))

LEFT JOIN medalion.db.dim_payer
  ON (slv_encounters.payer_id=dim_payer.payer_id
      AND dim_payer.start_date <= '{{ execution_date }}'
      AND (dim_payer.end_date > '{{ execution_date }}'
          OR dim_payer.is_active IS TRUE))

WHERE TO_DATE(slv_encounters.partition_date) = '{{ execution_date }}'