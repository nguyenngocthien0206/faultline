{#-
  Failpoints inside dbt's incremental strategies, for the crash-mid-write
  experiment (see coffee_platform/failpoints.py for failpoints in general).

  The two macros below are copies of dbt-clickhouse 1.10.3's own (they
  override them, root project macros win over the adapter's), unchanged but
  for one line each, marked "faultline":

    delete_insert     (naive gold)     after the DELETE, before the INSERT:
                      the old rows are gone, the new ones not yet in
    insert_overwrite  (hardened gold)  after the new data is built, before the
                      REPLACE PARTITION that swaps it in

  dbt runs in a subprocess, so it cannot kill the Dagster run itself: the
  failpoint raises with a marker, and the dbt asset, seeing the marker, dies
  at the failpoint of the same name. Only gold_daily_store_sales is armed, so
  the crash happens at exactly one place.

  Keep these in step with the installed dbt-clickhouse: a test fails when
  reference/dagster/requirements.lock pins another version.
-#}

{% macro faultline_failpoint(name) %}
  {%- if var('failpoint', '') == name and this.identifier == 'gold_daily_store_sales' -%}
    {{ exceptions.raise_compiler_error('faultline failpoint reached: ' ~ name) }}
  {%- endif -%}
{% endmacro %}

{% macro clickhouse__incremental_delete_insert(existing_relation, unique_key, incremental_predicates, is_distributed=False) %}
    {% set new_data_relation = existing_relation.incorporate(path={"identifier": existing_relation.identifier
       + '__dbt_new_data_' + invocation_id.replace('-', '_')}) %}
    {{ drop_relation_if_exists(new_data_relation) }}
    {%- set distributed_new_data_relation = existing_relation.incorporate(path={"identifier": existing_relation.identifier + '__dbt_distributed_new_data'}) -%}

    {%- set inserting_relation = new_data_relation -%}

    {%- set local_suffix = adapter.get_clickhouse_local_suffix() -%}
    {%- set local_db_prefix = adapter.get_clickhouse_local_db_prefix() -%}

    {% if is_distributed %}
      -- Need to use distributed table to have data on all shards
      {%- set inserting_relation = distributed_new_data_relation -%}
      {{ create_distributed_local_table(distributed_new_data_relation, new_data_relation, existing_relation, sql) }}
    {% else %}
      {% call statement('main') %}
        {{ get_create_table_as_sql(False, new_data_relation, sql) }}
      {% endcall %}
    {% endif %}

    {% call statement('delete_existing_data') %}
      {#- The delete mutation may run on a replica that has not yet synced the new data;
          embedding the setting in the subquery makes it read a fresh snapshot. -#}
      {%- set subquery_settings = ' settings select_sequential_consistency = 1' if target.database_engine == 'Shared' else '' -%}
      {% if is_distributed %}
          {% set existing_local = existing_relation.incorporate(path={"identifier": this.identifier + local_suffix, "schema": local_db_prefix + this.schema}) if existing_relation is not none else none %}
            delete from {{ existing_local }} {{ on_cluster_clause(existing_relation) }} where ({{ unique_key }}) in (select {{ unique_key }}
                                          from {{ inserting_relation }}{{ subquery_settings }})
      {% else %}
            delete from {{ existing_relation }} where ({{ unique_key }}) in (select {{ unique_key }}
                                          from {{ inserting_relation }}{{ subquery_settings }})
      {% endif %}
      {%- if incremental_predicates %}
        {% for predicate in incremental_predicates %}
            and {{ predicate }}
        {% endfor %}
      {%- endif -%}
      {{ adapter.get_model_query_settings(model) }}
    {% endcall %}

    {{ faultline_failpoint('gold_mid_write') }}  {#- faultline: between DELETE and INSERT -#}

    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {% call statement('insert_new_data') %}
        insert into {{ existing_relation }} select {{ dest_cols_csv }} from {{ inserting_relation }} {{ adapter.get_model_query_settings(model) }}
    {% endcall %}
    {% do adapter.drop_relation(new_data_relation) %}
    {{ drop_relation_if_exists(distributed_new_data_relation) }}
{% endmacro %}

{% macro clickhouse__incremental_insert_overwrite(existing_relation, partition_by, is_distributed=False) %}
    {% set new_data_relation = existing_relation.incorporate(path={"identifier": existing_relation.identifier
       + '__dbt_new_data_' + invocation_id.replace('-', '_')}) %}
    {{ drop_relation_if_exists(new_data_relation) }}
    {%- set distributed_new_data_relation = existing_relation.incorporate(path={"identifier": existing_relation.identifier + '__dbt_distributed_new_data'}) -%}


    {%- set local_suffix = adapter.get_clickhouse_local_suffix() -%}
    {%- set local_db_prefix = adapter.get_clickhouse_local_db_prefix() -%}
    {% set existing_local = existing_relation.incorporate(path={"identifier": this.identifier + local_suffix, "schema": local_db_prefix + this.schema}) if existing_relation is not none else none %}

    {% if is_distributed %}
        {{ create_distributed_local_table(distributed_new_data_relation, new_data_relation, existing_relation, sql) }}
    {% else %}
        {% call statement('main') %}
            {{ get_create_table_as_sql(False, new_data_relation, sql) }}
        {% endcall %}
    {% endif %}

    {{ faultline_failpoint('gold_mid_write') }}  {#- faultline: new data built, not swapped in -#}

    {# Get the parts from the cluster table, since the partitions between shards may not overlap due to distribution #}
    {% if execute %}
      {% set select_changed_partitions %}
          SELECT DISTINCT partition_id
          {% if is_distributed %}
            FROM cluster({{ adapter.get_clickhouse_cluster_name() }}, system.parts)
          {% else %}
            FROM system.parts
          {% endif %}
          WHERE active
            AND database = '{{ new_data_relation.schema }}'
            AND table = '{{ new_data_relation.identifier }}'
      {% endset %}
      {% set changed_partitions = run_query(select_changed_partitions).rows %}
    {% else %}
      {% set changed_partitions = [] %}
    {% endif %}

    {% if changed_partitions %}
        {% call statement('replace_partitions') %}
            {% if is_distributed %}
                alter table {{ existing_local }} {{ on_cluster_clause(existing_relation) }}
            {% else %}
                 alter table {{ existing_relation }}
            {% endif %}
            {%- for partition in changed_partitions %}
                replace partition id '{{ partition['partition_id'] }}'
                from {{ new_data_relation }}
                {{- ', ' if not loop.last }}
            {%- endfor %}
            {#- The strategy relies on REPLACE PARTITION from a source with no parts in the partition
                to clear shards that received no new data. ClickHouse 26.6+ refuses that by default,
                so opt back in. Older servers don't know the setting and would reject the query. -#}
            {%- if adapter.is_at_or_after_version('26.6') %}
                settings allow_replace_partition_from_empty_source = 1
            {%- endif %}
      {% endcall %}
    {% endif %}

    {% do adapter.drop_relation(distributed_new_data_relation) %}
    {% do adapter.drop_relation(new_data_relation) %}
{% endmacro %}
