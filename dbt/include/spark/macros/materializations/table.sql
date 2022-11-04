{% materialization table, adapter = 'spark' %}

  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- setup: if the target relation already exists, drop it
  -- in case if the existing and future table is delta or iceberg, we want to do a
  -- create or replace table instead of dropping, so we don't have the table unavailable
  {% if old_relation and not (old_relation.is_delta and config.get('file_format', validator=validation.any[basestring]) == 'delta') and not (old_relation.is_iceberg and config.get('file_format', validator=validation.any[basestring]) == 'iceberg')  -%}
    {{ adapter.drop_relation(old_relation) }}
  {%- endif %}

  {{ switch_catalog_create_table_hive(sql) }}
  -- build model
  {% call statement('main') -%}
    {{ dbt_common_create_table_as(False, target_relation, sql) }}
  {%- endcall %}
  
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}

{% macro dbt_common_create_table_as(temporary, relation, sql) -%}
  {%- set catalog_relation_name = get_catalog_relation_name(relation) -%}
  {% do log("catalog_relation_name: " ~  catalog_relation_name, info=True) %}

  {% if temporary -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    {% if config.get('file_format', validator=validation.any[basestring]) == 'delta' or config.get('file_format', validator=validation.any[basestring]) == 'iceberg' %}
      create or replace table {{ catalog_relation_name }}
    {% else %}
      create table {{ catalog_relation_name }}
    {% endif %}
    {{ file_format_clause() }}
    {{ partition_cols(label="partitioned by") }}
    {{ clustered_cols(label="clustered by") }}
    {{ location_clause() }}
    {{ comment_clause() }}
    as
      {{ sql }}
  {%- endif %}
{%- endmacro -%}
