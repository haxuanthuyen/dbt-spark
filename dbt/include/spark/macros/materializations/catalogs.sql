{% macro switch_catalog_create_table_hive(final_sql) -%}
  {%- set identifier = model['alias'] -%}
  {%- set catalog_type = config.get('catalog_type', 'hive') -%}
  {%- set catalog_name = config.get('catalog_name', 'spark_catalog') -%}
  {%- set sql_addons = 'create table if not exists spark_catalog.' + schema + '.' + identifier + ' using iceberg as ' + final_sql + ' limit 0' -%}
  {% if catalog_type == 'hadoop' -%}
    {% do log("execute query create table: " ~  sql_addons, info=True) %}
    {% do run_query(sql_addons) %}
  {%- endif %}
{%- endmacro -%}