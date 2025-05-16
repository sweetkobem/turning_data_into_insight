{{ config(
    materialized='custom_incremental_iceberg'
) }}

{% set execution_date = var('execution_date') %}
{% set md5_column = var('md5_column') %}

{% set result = run_query("SHOW TABLES in "~ target.schema ~" LIKE '"~ this.name ~"'") %}
{% if result and result.rows %}
    {% set table_exists = result.rows[0]['tableName'] %}
{% else %}
    {% set table_exists = none %}
{% endif %}

{% if table_exists %}
  {% set max_end_date = run_query("SELECT CAST(MAX(start_date) AS STRING) FROM "~ this ~" WHERE is_active = TRUE").columns[0][0] %}
{% else %}
  {% set max_end_date = None %}
{% endif %}

{% do log("max_end_date:"~max_end_date, info=True) %}

-- Check if max_end_date NULL or execution_date > max_end_date insert or update
{% if max_end_date is none or execution_date > max_end_date %}
  -- Insert the new records and old records
  WITH new_data AS (
    SELECT
        uuid() AS dim_patient_key,
        patient_id,
        patient_fullname,
        birth_date,
        death_date,
        marital_status,
        race,
        ethnicity,
        gender,
        birth_place,
        address,
        city,
        state,
        county,
        zip,
        CAST('{{ execution_date }}' AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        TRUE AS is_active

    FROM medalion.db.slv_patients
    {% if max_end_date %}
      WHERE {{ md5_column }} NOT IN (SELECT {{ md5_column }} FROM {{ this }} WHERE is_active = TRUE)  -- Avoid duplicates in incremental loads
    {% endif %}

  )
  {% if max_end_date %}
  , old_data AS (
    SELECT
      dim_patient_key,
      patient_id,
      patient_fullname,
      birth_date,
      death_date,
      marital_status,
      race,
      ethnicity,
      gender,
      birth_place,
      address,
      city,
      state,
      county,
      zip,
      start_date,
      CAST('{{ execution_date }}' AS DATE) AS end_date,
      FALSE AS is_active

    FROM {{ this }}
    WHERE patient_id IN (SELECT DISTINCT patient_id FROM medalion.db.slv_patients)
    AND end_date IS NULL

  )
  {% endif %}
  SELECT * FROM new_data
  {% if max_end_date %}
    UNION ALL
    SELECT * FROM old_data
  {% endif %}

{% else %}
  SELECT
      *
  FROM {{ this }}
  WHERE FALSE
{% endif %}
