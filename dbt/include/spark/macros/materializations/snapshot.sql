{% macro spark__snapshot_hash_arguments(args) -%}
    md5({%- for arg in args -%}
        coalesce(cast({{ arg }} as string ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}


{% macro spark__snapshot_string_as_time(timestamp) -%}
    {%- set result = "to_timestamp('" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}


{% macro spark__snapshot_merge_sql(target, source, insert_cols) -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source.identifier }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id
    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert *
    ;
{% endmacro %}


{% macro spark_build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_identifier = target_relation.identifier ~ '__dbt_tmp' %}
                                
    {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                  schema=none,
                                                  database=none,
                                                  type='view') -%}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {# needs to be a non-temp view so that its columns can be ascertained via `describe` #}
    {% call statement('build_snapshot_staging_relation') %}
        {{ create_view_as(tmp_relation, select) }}
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}


{% macro spark__post_snapshot(staging_relation) %}
    {% do adapter.drop_relation(staging_relation) %}
{% endmacro %}


{% macro spark__create_columns(relation, columns) %}
    {%- set catalog_type = config.get('catalog_type', 'hive') -%}
    {%- set catalog_name = config.get('catalog_name', 'spark_catalog') -%}
    {%- set relation_name = catalog_name + '.' + relation.schema + '.' + relation.identifier -%}
    {%- set default_relation_name =  'spark_catalog.' + relation.schema + '.' + relation.identifier -%}

    {% if columns|length > 0 %}
    {% call statement() %}
      alter table {{ relation_name }} add columns (
        {% for column in columns %}
          `{{ column.name }}` {{ column.data_type }} {{- ',' if not loop.last -}}
        {% endfor %}
      );
    {% endcall %}
    {% if catalog_type == 'hadoop' %}
        {% call statement() %}
          alter table {{ default_relation_name }} add columns (
            {% for column in columns %}
              `{{ column.name }}` {{ column.data_type }} {{- ',' if not loop.last -}}
            {% endfor %}
          );
        {% endcall %}
    {% endif %}
    {% endif %}
{% endmacro %}

{% macro build_snapshot_table(strategy, sql) %}

    select 'insert' dbt_change_type,*,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

{% endmacro %}

{% materialization snapshot, adapter='spark' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}
  {%- set file_format = config.get('file_format', 'parquet') -%}
  {%- set catalog_name = config.get('catalog_name', 'spark_catalog') -%}
  {%- set catalog_schema = catalog_name + '.' + model.schema  -%}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=none,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- set catalog_target_relation = catalog_name + '.' + target_relation.schema + '.' + target_table -%}
  {% do log("catalog_target_relation: " ~  catalog_target_relation, info=True) %}

  {%- if file_format not in ['delta', 'hudi', 'iceberg'] -%}
    {% set invalid_format_msg -%}
      Invalid file format: {{ file_format }}
      Snapshot functionality requires file_format be set to 'delta' or 'hudi' or 'iceberg'
    {%- endset %}
    {% do exceptions.raise_compiler_error(invalid_format_msg) %}
  {% endif %}

  {%- if target_relation_exists -%}
    {%- if not target_relation.is_delta and not target_relation.is_hudi and not target_relation.is_iceberg -%}
      {% set invalid_format_msg -%}
        The existing table {{ model.schema }}.{{ target_table }} is in another format than 'delta' or 'hudi' or 'iceberg'
      {%- endset %}
      {% do exceptions.raise_compiler_error(invalid_format_msg) %}
    {% endif %}
  {% endif %}

  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
      {% set final_sql = create_table_as(False, catalog_target_relation, build_sql) %}

      {{ switch_catalog_create_table_hive(build_sql) }}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_table = spark_build_snapshot_staging_table(strategy, sql, target_relation) %}

      -- this may no-op if the database does not require column expansion
      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set from_columns = adapter.get_columns_in_relation(staging_table) %}
      {% set to_columns = adapter.get_columns_in_relation(target_relation) %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% set final_sql = snapshot_merge_sql(
            target = catalog_target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
