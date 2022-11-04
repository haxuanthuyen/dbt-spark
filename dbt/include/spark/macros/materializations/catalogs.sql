{% macro switch_catalog_create_table_hive(final_sql) -%}
  {%- set identifier = model['alias'] -%}
  {%- set catalog_type = config.get('catalog_type', 'hive') -%}
  {%- set catalog_name = config.get('catalog_name', 'spark_catalog') -%}
  {%- set schema_name = schema -%}
  {% if schema.split('.')|length > 1 -%}
    {%- set schema_name = schema.split('.')[1] -%}
  {%- endif %}
  {%- set sql_addons = 'create table if not exists spark_catalog.' + schema_name + '.' + identifier + ' using iceberg as ' + final_sql + ' limit 0' -%}
  {% if catalog_type == 'hadoop' -%}
    {% do log("execute query create table: " ~  sql_addons, info=True) %}
    {% do run_query(sql_addons) %}
  {%- endif %}
{%- endmacro -%}

{% macro get_catalog_relation_name(relation) -%}
    {%- set catalog_type = config.get('catalog_type', 'hive') -%}
    {%- set catalog_name = config.get('catalog_name', 'spark_catalog') -%}
    {%- set schema_name = config.get('schema', relation.schema) -%}
    {% if schema_name == None %}
        {%- set schema_name = relation.schema -%}
    {% endif %}

    {% do log("get_catalog_relation_name schema_name: " ~  schema_name, info=True) %}
    {% do log("get_catalog_relation_name relation: " ~  relation, info=True) %}
    {% do log("get_catalog_relation_name relation.schema: " ~  relation.schema, info=True) %}

    {%- set catalog_relation_name = catalog_name + '.' + schema_name + '.' + relation.identifier -%}
    {% if schema_name.split('.')|length > 1 -%}
        {% if catalog_type == 'hadoop' %}
            {%- set catalog_relation_name = schema_name + '.' + relation.identifier -%}
        {% else %}
            {% do log("get_catalog_relation_name model is wrong catalog config => switch to spark_catalog ", info=True) %}
            {%- set catalog_relation_name = 'spark_catalog.' + schema_name.split('.')[1] + '.' + relation.identifier -%}
        {% endif %}

    {%- endif %}
    {% do log("get_catalog_relation_name catalog_relation_name: " ~  catalog_relation_name, info=True) %}

    {% do return(catalog_relation_name) %}
{%- endmacro -%}