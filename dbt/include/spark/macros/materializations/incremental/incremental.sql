{% materialization incremental, adapter='spark' -%}
  {% do log("INFO: using dbt_common materialization"  , info=True) %}
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy', default='append') -%}

  {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {%- set unique_key = config.get('unique_key', none) -%}
  {#-- add this config to set column from source use to parttion in destination  --#}
  {%- set source_partition_column = config.get('source_partition_column', none) -%}
  {#-- whever cache staging view before merge or not DEFAULT =True DO CACHE  --#}
  {%- set is_cache_temp_table = config.get('is_cache_temp_table', True) -%}

  {%- set partition_by = config.get('partition_by', none) -%}

  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {%- set catalog_relation_name = get_catalog_relation_name(this) -%}

  {%- set catalog_target_relation = api.Relation.create(identifier=this.identifier,
                                                schema=catalog_relation_name,
                                                database=database,
                                                type='table') -%}

  {% do log("catalog_relation_name: " ~  catalog_target_relation, info=True) %}
  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {% if strategy == 'insert_overwrite' and partition_by %}
    {% call statement() %}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {% endcall %}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, catalog_target_relation, sql) %}
    {{ switch_catalog_create_table_hive(sql) }}
  {% elif existing_relation.is_view or full_refresh_mode %}
    {% do adapter.drop_relation(existing_relation) %}
    {% do adapter.drop_relation(catalog_target_relation) %}
    {% set build_sql = create_table_as(False, catalog_target_relation, sql) %}
    {{ switch_catalog_create_table_hive(sql) }}
  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}

    {#--get list of changed data parttion here--#}
    {% set  predicates = none %}
    {% if raw_strategy == 'merge' %}

      {% if is_cache_temp_table %}
        {#-- CACHE de COUNT so ban ghi + write xuong GCS --#}
        {% call statement(name, fetch_result=False) %}
          CACHE TABLE {{tmp_relation}} OPTIONS ('storageLevel' 'MEMORY_ONLY')
        {% endcall %}
      {% endif %}

      {% if source_partition_column is not none %}
        {% set list_partition = get_date_partition_list(tmp_relation.include(schema=false),source_partition_column) %}
        {% if list_partition %}
          {#--build condition filter on merge--#}
          {% set  predicates = "DBT_INTERNAL_DEST." + partition_by[0] + " IN (" + list_partition + ")" %}
        {% else %}
          {% set  predicates = 'no_partition_changed' %}
        {% endif %}
      {% else %}
        {% set has_data = get_scalar_value_from_query(query = 'select 1 from ' ~ tmp_relation.include(schema=false) ~ ' limit 1') %}

        {% if has_data == none or has_data == '' %}
          {% do log("debug: do not have data to merge" , info = True)%}
          {% set  predicates = 'no_partition_changed' %}
        {% endif %}
      {% endif %}
    {% endif %}

    {% set build_sql = 'select 1' if predicates == 'no_partition_changed' else dbt_spark_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, predicates) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
