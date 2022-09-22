{% macro get_date_partition_list(table,column_date) %}

	{%- call statement('get_date_partition_list', fetch_result=True) -%}
		select collect_set(created_date)
		from
		(
			select
				concat("date('"
					,DATE_FORMAT({{ column_date }},"yyyy-MM-dd")
					,"')"
				)created_date
			from {{ table }} a
		) a
	{%- endcall -%}

	{%- set query_result = load_result('get_date_partition_list') -%}

	{% if execute %}
		{%- set query_result_text = query_result['data'][0][0] -%}
	    {{ return(query_result_text.replace('[','').replace(']','').replace('"','')) }}
	{% endif %}

{% endmacro %}

{% macro get_scalar_value_from_query(query) %}

	{% set result = run_query(query) %}
    {{ return(result.columns[0].values()[0] | trim ) }}

{% endmacro %}

{% macro scd2_hash(field_list) %}

	{%- set fields = [] -%}

	{%- for field in field_list -%}

		{%- set _ = fields.append(
			"coalesce(cast(" ~ field ~ " as " ~ dbt_utils.type_string() ~ "), '')"
		) -%}

	{%- endfor -%}

	{{dbt_utils.hash(dbt_utils.concat(fields))}}

{% endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
