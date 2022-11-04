{% macro get_insert_overwrite_sql(source_relation, target_relation) %}
    {%- set catalog_relation_name = get_catalog_relation_name(target_relation) -%}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ catalog_relation_name }}
    {{ partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}


{% macro get_insert_into_sql(source_relation, target_relation) %}
    {%- set catalog_relation_name = get_catalog_relation_name(target_relation) -%}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ catalog_relation_name }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}

{#--using default get_merge_sql instead of implimentation from dbt_spark adapter--#}
{% macro get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}

    {%- set catalog_relation_name = get_catalog_relation_name(target) -%}

    {%- set predicates = [] if predicates is none else predicates.split("@$") -%}
    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% set unique_key_match %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endset %}
        {% do predicates.append(unique_key_match) %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ catalog_relation_name }} as DBT_INTERNAL_DEST
        using {{ source.include(schema=false) }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}

    {% if unique_key %}
    when matched then update set
        {% if update_columns -%}
          {% for column_name in update_columns -%}
              {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
              {%- if not loop.last %}, {%- endif %}
          {%- endfor %}
        {%- else %} * {% endif %}
    {% endif %}

    when not matched then insert *

{% endmacro %}

{% macro dbt_spark_get_incremental_sql(strategy, source, target, unique_key, predicates) %}
  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ get_insert_into_sql(source, target) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ get_insert_overwrite_sql(source, target) }}
  {%- elif strategy == 'merge' -%}
  {#-- merge all columns with databricks delta - schema changes are handled for us #}
  {#-- set dest_columns = none to avoid error with default value when call get_merge_sql #}
  {% set dest_columns = none %}

    {{ get_merge_sql(target, source, unique_key, dest_columns, predicates) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}

{% endmacro %}
