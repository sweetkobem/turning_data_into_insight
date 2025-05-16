{{ config(
    materialized='custom_incremental_iceberg',
    partition_by='date'
) }}

{% set execution_date = var('execution_date') %}

SELECT
  fact_encounter.date,
  fact_encounter.dim_patient_key,
  fact_encounter.dim_payer_key,
  fact_encounter.encounter_class,
  CASE WHEN (fact_encounter.reason_description IS NOT NULL)
    THEN fact_encounter.reason_description
  ELSE fact_encounter.description END AS purpose,
  COUNT(DISTINCT fact_encounter.encounter_id) AS total_encounter_num,
  COUNT(fact_procedure.encounter_id) AS total_procedure_num,
  COUNT(fact_procedure.code) AS total_diagnosis_num,
  SUM(fact_encounter.claim_amount) AS total_claim_amount,
  SUM(fact_encounter.coverage_amount) AS total_coverage_amount,
  SUM(fact_encounter.patient_pay_amount) AS total_patient_pay_amount

FROM medalion.db.fact_encounter
LEFT JOIN medalion.db.fact_procedure
  ON (fact_encounter.encounter_id=fact_procedure.encounter_id
      AND fact_encounter.date=fact_procedure.date)

WHERE fact_encounter.date = '{{ execution_date }}'
GROUP BY 1,2,3,4,5