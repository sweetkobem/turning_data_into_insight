{%- materialization custom_incremental_iceberg, adapter = "default" -%}

    {%- set unique_key = config.get('unique_key') -%}
    {%- set partition_by = config.get('partition_by') -%}
    {%- set target_schema = target.schema -%}
    {%- set execution_date = var('execution_date') -%}

    -- Step 1: Check if table exists.
    {% set schema_exists_sql %}
        SHOW TABLES in {{ target.schema }} LIKE '{{ this.name }}';
    {% endset %}
    
    {% set result = run_query(schema_exists_sql) %}
    {% if result and result.rows %}
        {% set table_exists = result.rows[0]['tableName'] %}
    {% else %}
        {% set table_exists = none %}
    {% endif %}

    -- Step 2: Handle table creation on first run.
    {% if table_exists is none %}
        {% do log("Create table based on model.", info=True) %}
        {% set build_sql %}
        CREATE TABLE IF NOT EXISTS {{ this }}
        USING ICEBERG
        {% if partition_by %}
            PARTITIONED BY ({{ partition_by }})
        {% endif %}
        AS
        {{ compiled_code }}
        {% endset %}

    -- Step 3: If table exists do incremental load (INSERT OVERWRITE).
    {% else %}

        {% if partition_by %}
            -- Using INSERT OVERWRITE TABLE,
            -- we must exclude column partition_by,
            -- to avoid double column in query.
            {% set get_column_sql %}
                DESCRIBE {{ this }}
            {% endset %}
            {% set get_column = run_query(get_column_sql) %}
            {% set column = "" %}
            {% if get_column and get_column.rows %}
                {% set columns = [] %}
                {% for row in get_column.rows %}
                    {% if row[0] not in ['# Partition Information', '# col_name', partition_by] %}
                        {% set _ = columns.append(row[0]) %}
                    {% endif %}
                {% endfor %}
                {% set column = columns | join(', ') %}
                {% do log("columns:" ~columns, info=True) %}
            {% endif %}
        {% else %}
            {% set column = "*" %}
        {% endif %}

        {% do log("Running incremental load...", info=True) %}
        {% set build_sql %}
            {% if partition_by %}
                -- USING INSERT OVERWRITE
                INSERT OVERWRITE TABLE {{ this }}
                PARTITION ({{ partition_by }} = '{{ execution_date }}')
            {% else %}
                -- USING INSERT/APPEND
                INSERT INTO {{ this }}
            {% endif %}
            SELECT
                {{ column }}
            FROM ({{ compiled_code }})
        {% endset %}
        {% do log("build_sql:"~build_sql, info=True) %}

    {% endif %}

    -- Run pre hooks
    {{ run_hooks(pre_hooks) }}

    -- Execute SQL
    {% call statement("main") %} {{ build_sql }} {% endcall %}

    -- Run pre hooks
    {{ run_hooks(post_hooks) }}

    {{ return({"relations": [this]}) }}
{% endmaterialization %}
