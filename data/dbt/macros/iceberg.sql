{#
  One model, built in DuckDB and committed to Iceberg.

  dbt-duckdb reaches a store that is not DuckDB through a plugin, and the
  adapter hands that plugin a staged file rather than a cursor. So this stages
  the model as one Parquet file and calls the plugin with it: `rekep.dbt` reads
  it back as an Arrow stream and commits it through the same dataset every task
  writes through, under the keys, partition and storage types the model's own
  configuration declares.

  The DuckDB table stays: `ref()` reads it inside the same build, and nothing
  survives the process because the database is `:memory:`.

  `external` is the materialization dbt-duckdb ships for this shape. It is not
  what this uses: it writes a row of nulls for an empty model so a file always
  has a schema, and a row of nulls is not a row this would commit.
#}
{% materialization iceberg, adapter="duckdb", supported_languages=["sql"] %}

  {%- set plugin_name = config.get("plugin", "rekep") -%}
  {%- set staged = render(config.get("location", default=external_location(this, config))) -%}
  {%- set target_relation = this.incorporate(type="table") -%}
  {%- set existing_relation = load_cached_relation(this) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is not none %}
    {{ adapter.drop_relation(existing_relation) }}
  {% endif %}

  {% call statement("main") -%}
    {{ create_table_as(False, target_relation, compiled_code) }}
  {%- endcall %}

  {{ write_to_file(target_relation, staged, "format parquet") }}
  {% do store_relation(plugin_name, target_relation, staged, "parquet", config) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({"relations": [target_relation]}) }}

{% endmaterialization %}
