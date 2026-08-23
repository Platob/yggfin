# Airflow

The DAG runs each adjacent YAML/notebook pair with
`apache-airflow-providers-papermill`; `rekep` itself has no Airflow runtime
dependency.

```bash
pip install "rekep[iceberg,yaml]" apache-airflow-providers-papermill
export REKEP_ROOT=/opt/rekep
mkdir -p /tmp/rekep-notebooks
airflow dags test rekep_market_pipeline 2026-08-21T10:00:00Z
```

Set `REKEP_NOTEBOOK_OUTPUT` to an existing worker-visible directory when
`/tmp/rekep-notebooks` is unsuitable. Airflow injects its half-open data
interval as `start` and `end`; Arrow and Iceberg carry data between notebooks.

```text
parse_logs -+-> flatten_instruments
            `-> parse_market -+-> flatten_orders
                              `-> flatten_executions
```
