# MarimoOperator

`marimo_operator.py` is the reusable Airflow operator for a Rekep task
document. This repository no longer ships the old FIX/market DAG.

One operator call executes:

```text
uv run --project <repository>/python --group runner --no-sync --offline \
  rekep task run <repository>/tasks/parse_messages/parse_messages.yml \
  --parameters-file <attempt>/parameters.json \
  --result-file <attempt>/result.json
```

The operator publishes the validated task result under XCom `return_value` and
removes its private attempt directory whether the application succeeds or
fails. A deployment may wrap `parse_messages` in its own DAG and provide an
exact immutable filesystem prefix for each interval.
