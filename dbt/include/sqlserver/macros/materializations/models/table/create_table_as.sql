
{% macro sqlserver__create_table_as(temporary, relation, sql) -%}
    {#- TODO: add contracts here when in dbt 1.5 -#}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set as_columnstore = config.get('as_columnstore', default=true) -%}
    {%- set temp_view_sql = sql.replace("'", "''") -%}

    {{- sql_header if sql_header is not none -}}

    -- select into the table and create it that way
    use [{{ relation.database }}];
    exec('
        select
            *
        into
            {{ relation.include(database=False, schema=(not temporary)) }}
        from
            {{ temp_view_sql }}
    ');

    {%- if not temporary and as_columnstore -%}
        -- add columnstore index
        {{ sqlserver__create_clustered_columnstore_index(relation) }}
    {%- endif -%}
{% endmacro %}
