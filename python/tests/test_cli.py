import pathlib

import pytest

from rekep.cli import main

NAMESPACE = "rekep.models.Log"


def dump(*extra: str) -> int:
    return main(["service", "ddl", "dump", "--namespace", NAMESPACE, *extra])


def test_ddl_dump_writes_the_file(tmp_path: pathlib.Path, capsys: pytest.CaptureFixture) -> None:
    assert dump("--out", str(tmp_path)) == 0
    written = tmp_path / "log.sql"
    assert written.exists()
    assert str(written) in capsys.readouterr().out
    assert "CREATE TABLE IF NOT EXISTS iceberg.default.log (" in written.read_text()


def test_table_name_names_table_and_file(tmp_path: pathlib.Path) -> None:
    dump("--table-name", "logs", "--out", str(tmp_path))
    assert (
        "CREATE TABLE IF NOT EXISTS iceberg.default.logs (" in (tmp_path / "logs.sql").read_text()
    )


def test_stdout_dump(capsys: pytest.CaptureFixture) -> None:
    assert dump("--out", "-") == 0
    assert "USING iceberg" in capsys.readouterr().out


def test_jinja_renders_from_environment(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATA_BUCKET", "s3://lake")
    dump("--location", "{{ env.DATA_BUCKET }}/logs", "--out", str(tmp_path))
    assert "LOCATION 's3://lake/logs'" in (tmp_path / "log.sql").read_text()


def test_jinja_renders_from_vars_and_args(tmp_path: pathlib.Path) -> None:
    dump(
        "--var",
        "zone=eu",
        "--property",
        "comment={{ zone }}:{{ namespace }}",
        "--out",
        str(tmp_path),
    )
    assert "'comment' = 'eu:rekep.models.Log'" in (tmp_path / "log.sql").read_text()


def test_partition_by_flows_through(tmp_path: pathlib.Path) -> None:
    dump("--partition-by", "driver", "--out", str(tmp_path))
    assert "PARTITIONED BY (driver)" in (tmp_path / "log.sql").read_text()


def test_a_non_record_namespace_is_refused() -> None:
    with pytest.raises(SystemExit, match="not a Record"):
        main(["service", "ddl", "dump", "--namespace", "pathlib.Path", "--out", "-"])


def test_a_malformed_property_is_refused() -> None:
    with pytest.raises(SystemExit, match="KEY=VALUE"):
        dump("--property", "oops", "--out", "-")


# -- product ----------------------------------------------------------------


def test_product_dump_defaults_to_yaml(tmp_path: pathlib.Path) -> None:
    assert (
        main(["service", "product", "dump", "--namespace", NAMESPACE, "--out", str(tmp_path)]) == 0
    )
    written = tmp_path / "log.yaml"
    assert written.exists()
    payload = written.read_bytes()
    assert payload.startswith(b"name: Log")
    assert b"namespace:" not in payload


def test_product_dump_other_formats(tmp_path: pathlib.Path) -> None:
    main(
        [
            "service",
            "product",
            "dump",
            "--namespace",
            NAMESPACE,
            "--format",
            "json",
            "--out",
            str(tmp_path),
        ]
    )
    assert (tmp_path / "log.json").read_bytes().startswith(b"{")


def test_product_dump_to_stdout(capsys: pytest.CaptureFixture) -> None:
    assert main(["service", "product", "dump", "--namespace", NAMESPACE, "--out", "-"]) == 0
    assert "fields:" in capsys.readouterr().out


# -- git context in jinja ---------------------------------------------------


def test_git_branch_suffix_renders_in_names(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.render import git_context

    monkeypatch.setenv("GITHUB_REF_NAME", "feature/x-1")
    git_context.cache_clear()
    try:
        dump("--table-name", "logs{{ git_branch_suffix }}", "--out", str(tmp_path))
        written = tmp_path / "logs_feature_x_1.sql"
        assert "iceberg.default.logs_feature_x_1 (" in written.read_text()
    finally:
        git_context.cache_clear()


def test_git_suffix_is_empty_on_trunk(monkeypatch: pytest.MonkeyPatch) -> None:
    from rekep.render import git_context

    monkeypatch.setenv("GITHUB_REF_NAME", "main")
    git_context.cache_clear()
    try:
        assert git_context()["git_branch_suffix"] == ""
        assert git_context()["git_branch_prefix"] == ""
    finally:
        git_context.cache_clear()


# -- dry run ----------------------------------------------------------------


def test_iceberg_deploy_dry_run_touches_nothing(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
) -> None:
    root = tmp_path.as_posix()
    catalogs = tmp_path / "catalogs"
    catalogs.mkdir()
    (catalogs / "iceberg.yaml").write_text(
        f'type: sql\nuri: "sqlite:///{root}/cat.db"\nwarehouse: "file://{root}/wh"\n'
    )
    assert main(["service", "iceberg", "deploy", "--config", str(tmp_path), "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "would converge" in out
    assert not (tmp_path / "wh").exists(), "nothing materialised"


# -- records deploy ---------------------------------------------------------


def workspace(tmp_path: pathlib.Path) -> pathlib.Path:
    """A fully local Iceberg registry in a scratch folder."""
    root = tmp_path.as_posix()
    catalogs = tmp_path / "catalogs"
    catalogs.mkdir()
    (catalogs / "iceberg.yaml").write_text(
        f'type: sql\nuri: "sqlite:///{root}/cat.db"\nwarehouse: "file://{root}/wh"\n'
    )
    return tmp_path


def test_records_deploy_converges_one_record(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
) -> None:
    root = workspace(tmp_path)
    assert (
        main(
            [
                "service",
                "records",
                "deploy",
                "--pyclass",
                NAMESPACE,
                "--target",
                "iceberg",
                "--config",
                str(root),
            ]
        )
        == 0
    )
    assert "iceberg: default.log" in capsys.readouterr().out

    from rekep.iceberg import Iceberg

    stack = Iceberg.load(root)
    assert stack.catalogs.connect("iceberg").table_exists("default.log")


def test_records_deploy_dry_run_touches_nothing(tmp_path: pathlib.Path) -> None:
    root = workspace(tmp_path)
    assert (
        main(
            [
                "service",
                "records",
                "deploy",
                "--pyclass",
                NAMESPACE,
                "--config",
                str(root),
                "--dry-run",
            ]
        )
        == 0
    )
    assert not (tmp_path / "wh").exists()


def test_records_deploy_doris_emits_the_plan(capsys: pytest.CaptureFixture) -> None:
    assert (
        main(
            [
                "service",
                "records",
                "deploy",
                "--pyclass",
                NAMESPACE,
                "--target",
                "doris",
                "--dry-run",
            ]
        )
        == 0
    )
    assert "CREATE TABLE IF NOT EXISTS iceberg.default.log (" in capsys.readouterr().out


def test_records_deploy_refuses_a_non_record() -> None:
    with pytest.raises(SystemExit, match="not a Record"):
        main(["service", "records", "deploy", "--pyclass", "pathlib.Path", "--dry-run"])


# -- registry sync ----------------------------------------------------------


def registry(tmp_path: pathlib.Path) -> pathlib.Path:
    """A minimal iceberg registry: each file states only its overrides."""
    for folder, name, body in (
        ("catalogs", "iceberg", "type: sql\n"),
        ("namespaces", "default", "catalog: iceberg\n"),
    ):
        (tmp_path / folder).mkdir(exist_ok=True)
        (tmp_path / folder / f"{name}.yaml").write_text(body)
    return tmp_path


def test_sync_writes_every_registry_in_full(tmp_path: pathlib.Path) -> None:
    import yaml

    root = registry(tmp_path)
    assert main(["service", "iceberg", "sync", "--config", str(root)]) == 0

    catalog = yaml.safe_load((root / "catalogs" / "iceberg.yaml").read_bytes())
    assert catalog["name"] == "iceberg", "the stem-defaulted name is written out"
    assert catalog["uri"].startswith("sqlite:///"), "defaults are materialised too"

    namespace = yaml.safe_load((root / "namespaces" / "default.yaml").read_bytes())
    assert namespace["name"] == "default"
    assert namespace["catalog"] == "iceberg"
    assert not (root / "tables").exists(), "no tables/ folder any more"


def test_sync_is_idempotent(tmp_path: pathlib.Path) -> None:
    root = registry(tmp_path)
    main(["service", "iceberg", "sync", "--config", str(root)])
    assert main(["service", "iceberg", "sync", "--config", str(root), "--dry-run"]) == 0


def test_sync_leaves_templated_files_alone(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
) -> None:
    """Rewriting a template would bake this machine's environment into it."""
    root = registry(tmp_path)
    templated = root / "namespaces" / "default.yaml"
    templated.write_text("catalog: iceberg\nlocation: \"{{ env.get('X', '/tmp') }}\"\n")
    before = templated.read_bytes()

    assert main(["service", "iceberg", "sync", "--config", str(root)]) == 0
    assert templated.read_bytes() == before
    assert "templated" in capsys.readouterr().out


# -- dataset deploy -----------------------------------------------------------


def dataset_workspace(tmp_path: pathlib.Path) -> pathlib.Path:
    """A fully local Iceberg registry plus one declared dataset, in one folder."""
    root = tmp_path.as_posix()
    catalogs = tmp_path / "iceberg" / "catalogs"
    catalogs.mkdir(parents=True)
    (catalogs / "iceberg.yaml").write_text(
        f'type: sql\nuri: "sqlite:///{root}/cat.db"\nwarehouse: "file://{root}/wh"\n'
    )
    datasets = tmp_path / "datasets"
    datasets.mkdir()
    (datasets / "logs.yaml").write_text("record: rekep.models.Log\n")
    return tmp_path


def test_dataset_deploy_converges_the_declared_dataset(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
) -> None:
    root = dataset_workspace(tmp_path)
    assert (
        main(
            [
                "service",
                "dataset",
                "deploy",
                "--config",
                str(root / "datasets"),
                "--stack-config",
                str(root / "iceberg"),
                "--target",
                "iceberg",
            ]
        )
        == 0
    )
    assert "dataset://default/logs" in capsys.readouterr().out

    from rekep.iceberg import Iceberg

    stack = Iceberg.load(root / "iceberg")
    assert stack.catalogs.connect("iceberg").table_exists("default.logs")


def test_dataset_deploy_dry_run_touches_nothing(tmp_path: pathlib.Path) -> None:
    root = dataset_workspace(tmp_path)
    assert (
        main(
            [
                "service",
                "dataset",
                "deploy",
                "--config",
                str(root / "datasets"),
                "--stack-config",
                str(root / "iceberg"),
                "--dry-run",
            ]
        )
        == 0
    )
    assert not (root / "iceberg" / "wh").exists()


def test_dataset_list_prints_declared_datasets(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
) -> None:
    root = dataset_workspace(tmp_path)
    assert main(["service", "dataset", "list", "--config", str(root / "datasets")]) == 0
    out = capsys.readouterr().out
    assert "dataset://default/logs" in out
    assert "record=rekep.models.Log" in out


# -- dataset maintain ---------------------------------------------------------


def crowded_dataset_workspace(tmp_path: pathlib.Path) -> pathlib.Path:
    """A deployed dataset whose table already holds too many small files."""
    import datetime

    import pyarrow

    from rekep.dataset import Dataset
    from rekep.iceberg import Iceberg
    from rekep.models import ParsedMessage

    root = dataset_workspace(tmp_path)
    (root / "datasets" / "logs.yaml").unlink()
    (root / "datasets" / "messages.yaml").write_text(
        "record: rekep.models.ParsedMessage\n"
        "name: messages\n"
        "protocols:\n"
        "  iceberg:\n"
        '    compact_min_files: "3"\n'
        "    retain: 0s\n"
    )
    main(
        [
            "service",
            "dataset",
            "deploy",
            "--config",
            str(root / "datasets"),
            "--stack-config",
            str(root / "iceberg"),
        ]
    )
    stack = Iceberg.load(root / "iceberg")
    (dataset,) = Dataset.load_all(root / "datasets")
    table = stack.tables.get(dataset.into_iceberg_table())
    schema = ParsedMessage.into_arrow_schema()
    for index in range(4):
        table.append(
            pyarrow.Table.from_pylist(
                [
                    {
                        "url": "u",
                        "unix": index,
                        "date": datetime.date(2026, 8, 14),
                        "hash64": index,
                        "protocol": None,
                        "fields": {},
                    }
                ],
                schema=schema,
            )
        )
    return root


def test_dataset_maintain_dry_run_reports_without_rewriting(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
) -> None:
    root = crowded_dataset_workspace(tmp_path)
    assert (
        main(
            [
                "service",
                "dataset",
                "maintain",
                "--config",
                str(root / "datasets"),
                "--stack-config",
                str(root / "iceberg"),
                "--dry-run",
            ]
        )
        == 0
    )
    assert "would rewrite 4 files in 1 partitions" in capsys.readouterr().out

    from rekep.dataset import Dataset
    from rekep.iceberg import Iceberg

    stack = Iceberg.load(root / "iceberg")
    (dataset,) = Dataset.load_all(root / "datasets")
    table = stack.tables.get(dataset.into_iceberg_table())
    assert table.inspect.data_files().num_rows == 4, "dry run rewrote nothing"


def test_dataset_maintain_compacts_and_expires_from_the_side_file(
    tmp_path: pathlib.Path, capsys: pytest.CaptureFixture
) -> None:
    """`compact_min_files` and `retain` are the whole policy -- no arguments."""
    root = crowded_dataset_workspace(tmp_path)
    assert (
        main(
            [
                "service",
                "dataset",
                "maintain",
                "--config",
                str(root / "datasets"),
                "--stack-config",
                str(root / "iceberg"),
            ]
        )
        == 0
    )
    assert "rewrote 4 files in 1 partitions" in capsys.readouterr().out

    from rekep.dataset import Dataset
    from rekep.iceberg import Iceberg

    stack = Iceberg.load(root / "iceberg")
    (dataset,) = Dataset.load_all(root / "datasets")
    table = stack.tables.get(dataset.into_iceberg_table())
    assert table.inspect.data_files().num_rows == 1, "one file per partition now"
    assert table.scan().to_arrow().num_rows == 4, "same rows"
    assert len(table.snapshots()) == 1, "retain: 0s dropped the history behind it"
