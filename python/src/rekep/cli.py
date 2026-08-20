"""The rekep command line, one service class per capability.

Every command hangs off a service (`rekep ddl dump ...`, `rekep dataset
deploy ...`), so a new capability is a new service class here rather than a
new flag on an old one. Each class registers its own subparser directly on
the top level: the service *is* the command word, with no grouping noun in
front of it, so the shape a user types matches the shape the code is in.
String options may be Jinja templates -- `--location "s3://{{ env.BUCKET }}"`
-- rendered with the other arguments and the process environment before use.
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sys
from collections.abc import Sequence
from typing import Any

from rekep import config
from rekep.records import Record
from rekep.render import render

LOGO = r"""
  ____  _____ _  _______ ____
 |  _ \| ____| |/ / ____|  _ \
 | |_) |  _| | ' /|  _| | |_) |
 |  _ <| |___| . \| |___|  __/
 |_| \_\_____|_|\_\_____|_|
    one record, every lake
"""

#: Where generated DDL lands, relative to the deployment root.
DDL_ROOT = pathlib.Path("stacks/ddl/iceberg")


class DdlService:
    """`rekep ddl`: DDL emitted from record declarations."""

    name = "ddl"

    def register(self, commands: Any) -> None:
        parser = commands.add_parser(self.name, help="DDL from record declarations")
        commands = parser.add_subparsers(dest="command", required=True)

        dump = commands.add_parser("dump", help="write CREATE TABLE for a record")
        dump.add_argument(
            "--record", required=True, help="record uri or name, e.g. rekep:///records/log"
        )
        dump.add_argument(
            "--dialect",
            choices=("iceberg", "doris"),
            default="iceberg",
            help="SQL dialect (default: iceberg)",
        )
        dump.add_argument(
            "--config",
            default=None,
            help="deployment directory (default: data/<dialect>)",
        )
        dump.add_argument(
            "--table-name", default=None, help="table name; defaults to the record's snake name"
        )
        dump.add_argument(
            "--location", default=None, help="table LOCATION; may be a Jinja template"
        )
        dump.add_argument(
            "--partition-by",
            nargs="*",
            default=(),
            help="partition columns, overriding field metadata",
        )
        dump.add_argument(
            "--property",
            dest="properties",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="TBLPROPERTIES entry; repeatable",
        )
        dump.add_argument(
            "--var",
            dest="variables",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="extra Jinja variable; repeatable",
        )
        dump.add_argument("--out", default=str(DDL_ROOT), help="output directory, or - for stdout")
        dump.set_defaults(run=self.dump)

    def dump(self, arguments: argparse.Namespace) -> int:
        cls = _record_class(arguments.record)
        context = {
            "record": str(cls.record_uri()),
            "table_name": arguments.table_name,
            **_pairs(arguments.variables),
        }

        def templated(text: str | None) -> str | None:
            return render(text, **context) if text else text

        properties = {key: templated(value) for key, value in _pairs(arguments.properties).items()}
        table = templated(arguments.table_name)
        if arguments.dialect == "doris":
            from rekep.records.doris import DORIS_ROOT, DorisDeployment

            deployment = DorisDeployment.load(arguments.config or DORIS_ROOT, **context)
            ddl = deployment.ddl_for(cls, table_name=table, properties=properties or None)
            entry = deployment.table(cls)
            table = table or (entry.name if entry else None)  # name the file as deployed
        else:
            from rekep.records.iceberg import ICEBERG_ROOT, IcebergDeployment

            deployment = IcebergDeployment.load(arguments.config or ICEBERG_ROOT, **context)
            overrides = {
                "location": templated(arguments.location),
                "partitioned_by": arguments.partition_by or None,
                "properties": properties or None,
            }
            ddl = deployment.ddl_for(
                cls, table_name=table, **{k: v for k, v in overrides.items() if v}
            )
            entry = deployment.table(cls)
            table = table or (entry.name if entry else None)
        if arguments.out == "-":
            sys.stdout.write(ddl)
            return 0
        out = pathlib.Path(arguments.out)
        if out == DDL_ROOT:
            out = out.parent / arguments.dialect  # each dialect its own folder
        out.mkdir(parents=True, exist_ok=True)
        table = table or cls.record_name()
        path = out / f"{table.rpartition('.')[2]}.sql"
        path.write_text(ddl, encoding="utf-8", newline="\n")
        print(path)
        return 0


class DocsService:
    """`rekep docs`: documentation pages generated from the code."""

    name = "docs"

    def register(self, commands: Any) -> None:
        parser = commands.add_parser(self.name, help="generated documentation pages")
        commands = parser.add_subparsers(dest="command", required=True)

        models = commands.add_parser("models", help="write the data-models page")
        models.add_argument(
            "--out", default="docs/models.md", help="markdown file, or - for stdout"
        )
        models.set_defaults(run=self.models)

    def models(self, arguments: argparse.Namespace) -> int:
        """One section per model in `rekep.models`, from the live declarations.

        Generated, never edited: the page is a projection of the records, on
        the same terms as the Arrow schema or the DDL.
        """
        import rekep.models

        sections = [
            "# Data models",
            "",
            "*Generated by `rekep docs models` -- edit the records, not this page.*",
        ]
        for name in rekep.models.__all__:
            cls = getattr(rekep.models, name)
            sections.append(self.section(cls))
        page = "\n".join(sections) + "\n"
        if arguments.out == "-":
            sys.stdout.write(page)
            return 0
        out = pathlib.Path(arguments.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(page, encoding="utf-8", newline="\n")
        print(out)
        return 0

    def section(self, cls: type[Record]) -> str:
        described = cls.into_dict()
        lines = [
            "",
            f"## {described['name']}",
            "",
            f"`{cls.record_uri()}`",
            "",
        ]
        if described.get("description"):
            lines += [described["description"], ""]
        lines += [
            "| Field | Type | Required | Description | Iceberg |",
            "| --- | --- | --- | --- | --- |",
        ]
        for field in described["fields"]:
            iceberg = ", ".join(
                f"{key}={value}" for key, value in (field.get("iceberg") or {}).items()
            )
            lines.append(
                "| `{name}` | `{type}` | {required} | {description} | {iceberg} |".format(
                    name=field["name"],
                    type=field["type"],
                    required="" if field.get("nullable") else "yes",
                    description=field.get("description", ""),
                    iceberg=iceberg,
                )
            )
        return "\n".join(lines)


class StackService:
    """Shared CLI shape for the Doris and Iceberg deployment stacks."""

    name = "stack"
    root: pathlib.Path

    def register(self, commands: Any) -> None:
        parser = commands.add_parser(self.name, help=f"{self.name} deployment stack")
        commands = parser.add_subparsers(dest="command", required=True)

        deploy = commands.add_parser("deploy", help="converge catalogs and namespaces")
        self._common(deploy)
        deploy.add_argument(
            "--dry-run",
            action="store_true",
            help="log what would happen without touching anything",
        )
        deploy.set_defaults(run=self.deploy)

        whole = commands.add_parser("sync", help="rewrite every registry file in full")
        self._common(whole)
        whole.add_argument(
            "--dry-run",
            action="store_true",
            help="report files that would change; exit 1 when any would",
        )
        whole.set_defaults(run=self.sync, folders=None)

    def _common(self, parser: Any) -> None:
        parser.add_argument("--config", default=None, help="deployment directory")
        parser.add_argument(
            "--verbose", action="store_true", help="debug logging: every deployed detail"
        )
        parser.add_argument(
            "--var",
            dest="variables",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="extra Jinja variable; repeatable",
        )

    # -- shared verbs -------------------------------------------------------

    def _context(self, arguments: argparse.Namespace) -> dict[str, str]:
        level = logging.DEBUG if getattr(arguments, "verbose", False) else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s %(name)s %(message)s")
        return _pairs(arguments.variables)

    def sync(self, arguments: argparse.Namespace) -> int:
        """Rewrite registry files in full -- every field, `name` included.

        Each entry is loaded, defaulted (the file stem supplies `name`),
        materialised where the record knows how, and written back complete,
        so a side file states its whole contract instead of only its
        overrides.

        A file containing Jinja is left alone: rewriting it would resolve the
        template against *this* machine's environment and bake the answer in.
        """
        root = pathlib.Path(arguments.config or self.root)
        context = self._context(arguments)
        wanted = arguments.folders or tuple(self.registries())
        drifted = False
        for folder in wanted:
            drifted |= _sync_folder(
                root / folder,
                self.registries()[folder],
                context,
                dry_run=arguments.dry_run,
                stem_names=True,
            )
        return 1 if arguments.dry_run and drifted else 0


class IcebergService(StackService):
    """`rekep iceberg`: the Iceberg stack, resource by resource."""

    name = "iceberg"

    @property
    def root(self) -> pathlib.Path:
        from rekep.records.iceberg import ICEBERG_ROOT

        return ICEBERG_ROOT

    def registries(self) -> dict[str, Any]:
        from rekep.records.iceberg import IcebergCatalog, IcebergNamespace

        return {
            "catalogs": IcebergCatalog,
            "namespaces": IcebergNamespace,
        }

    def load(self, arguments: argparse.Namespace) -> Any:
        from rekep.iceberg import Iceberg

        return Iceberg.load(arguments.config or self.root, **self._context(arguments))

    def deploy(self, arguments: argparse.Namespace) -> int:
        done = self.load(arguments).deploy(dry_run=arguments.dry_run)
        header = "would converge" if arguments.dry_run else "converged"
        for level in ("catalogs", "namespaces", "tables"):
            print(f"{header} {level}: {', '.join(done[level]) or 'none'}")
        return 0


class DorisService(StackService):
    """`rekep doris`: the Doris stack, resource by resource."""

    name = "doris"

    @property
    def root(self) -> pathlib.Path:
        from rekep.records.doris import DORIS_ROOT

        return DORIS_ROOT

    def registries(self) -> dict[str, Any]:
        from rekep.records.doris import DorisCatalog, DorisNamespace

        return {
            "catalogs": DorisCatalog,
            "namespaces": DorisNamespace,
        }

    def load(self, arguments: argparse.Namespace) -> Any:
        from rekep.doris import Doris

        return Doris.load(arguments.config or self.root, **self._context(arguments))

    def deploy(self, arguments: argparse.Namespace) -> int:
        """Without a cluster connection the deploy IS the ordered plan."""
        for statement in self.load(arguments).deploy():
            sys.stdout.write(statement + "\n")
        return 0


class RecordsService:
    """`rekep records`: one record class, converged into the stacks."""

    name = "records"

    def register(self, commands: Any) -> None:
        parser = commands.add_parser(self.name, help="record classes: dump and deploy")
        commands = parser.add_subparsers(dest="command", required=True)

        dump = commands.add_parser("dump", help="write a record's whole declaration")
        dump.add_argument(
            "--record", required=True, help="record uri or name, e.g. rekep:///records/log"
        )
        dump.add_argument(
            "--format",
            choices=("yaml", "json", "toml"),
            default="yaml",
            help="output format (default: yaml)",
        )
        dump.add_argument("--out", default="-", help="output directory, or - for stdout")
        dump.set_defaults(run=self.dump)

        deploy = commands.add_parser("deploy", help="converge one record into its targets")
        deploy.add_argument(
            "--record", required=True, help="record uri or name, e.g. rekep:///records/log"
        )
        deploy.add_argument(
            "--target",
            dest="targets",
            action="append",
            choices=("iceberg", "doris"),
            help="stack to converge into; repeatable (default: iceberg)",
        )
        deploy.add_argument(
            "--config", default=None, help="deployment directory (default: stacks/<target>)"
        )
        deploy.add_argument(
            "--dry-run", action="store_true", help="log what would happen without touching anything"
        )
        deploy.add_argument(
            "--verbose", action="store_true", help="debug logging: every deployed detail"
        )
        deploy.add_argument(
            "--var",
            dest="variables",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="extra Jinja variable; repeatable",
        )
        deploy.set_defaults(run=self.deploy)

    def dump(self, arguments: argparse.Namespace) -> int:
        """One record class's whole declaration -- the contract, without rows.

        Goes to stdout by default and has no shipped folder of its own: a
        data product's declaration belongs *in its dataset side file*, which
        `rekep dataset sync` writes and CI drift-tests. This is the same view
        for a class nobody has declared a dataset for yet.
        """
        cls = _record_class(arguments.record)
        payload: bytes = getattr(cls, f"into_{arguments.format}")()
        if arguments.out == "-":
            sys.stdout.write(payload.decode())
            return 0
        out = pathlib.Path(arguments.out)
        out.mkdir(parents=True, exist_ok=True)
        path = out / f"{cls.record_name()}.{arguments.format}"
        path.write_bytes(payload)
        print(path)
        return 0

    def deploy(self, arguments: argparse.Namespace) -> int:
        """The record's table, per target: live for Iceberg, planned for Doris.

        The deployment's own table entry wins when one declares this record;
        otherwise the stack defaults apply -- exactly `ddl_for`'s rule, but
        converging instead of printing.
        """
        level = logging.DEBUG if arguments.verbose else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s %(name)s %(message)s")
        cls = _record_class(arguments.record)
        context = _pairs(arguments.variables)
        reference = str(cls.record_uri())

        for target in arguments.targets or ["iceberg"]:
            if target == "iceberg":
                from rekep.iceberg import Iceberg
                from rekep.records.iceberg import ICEBERG_ROOT, IcebergTable

                stack = Iceberg.load(arguments.config or ICEBERG_ROOT, **context)
                table = stack.deployment.table(cls) or IcebergTable(
                    record=reference, namespace=stack.deployment.namespaces[0].name
                )
                stack.deploy_one(table, dry_run=arguments.dry_run)
                print(f"iceberg: {stack.tables.identifier(table)}")
            else:
                from rekep.doris import Doris
                from rekep.records.doris import DORIS_ROOT, DorisTable

                stack = Doris.load(arguments.config or DORIS_ROOT, **context)
                table = stack.deployment.table(cls) or DorisTable(
                    record=reference, namespace=stack.deployment.namespaces[0].name
                )
                statement = stack.deploy_one(table, dry_run=arguments.dry_run)
                sys.stdout.write(statement or "")
        return 0


class DatasetService:
    """`rekep dataset`: datasets deployed autonomously, iceberg or doris.

    A `Dataset` needs no matching `IcebergTable`/`DorisTable` side file --
    its own `record`/`namespace`/`protocols` carry everything a table
    declaration used to. `deploy` builds one ad hoc per target
    (`into_iceberg_table`/`into_doris_table`) and converges it straight into
    that stack's `catalogs`/`namespaces`, the only folders those stacks
    declare any more.
    """

    name = "dataset"

    def register(self, commands: Any) -> None:
        parser = commands.add_parser(self.name, help="datasets deployed into iceberg or doris")
        commands = parser.add_subparsers(dest="command", required=True)

        deploy = commands.add_parser("deploy", help="converge every declared dataset")
        deploy.add_argument("--config", default=None, help="datasets directory")
        deploy.add_argument(
            "--target",
            dest="targets",
            action="append",
            choices=("iceberg", "doris"),
            help="stack to converge into; repeatable (default: iceberg)",
        )
        deploy.add_argument(
            "--stack-config",
            default=None,
            help="iceberg/doris deployment directory (default: stacks/<target>)",
        )
        deploy.add_argument(
            "--dry-run", action="store_true", help="log what would happen without touching anything"
        )
        deploy.add_argument(
            "--verbose", action="store_true", help="debug logging: every deployed detail"
        )
        deploy.add_argument(
            "--var",
            dest="variables",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="extra Jinja variable; repeatable",
        )
        deploy.set_defaults(run=self.deploy)

        maintain = commands.add_parser(
            "optimize", help="compact and reclaim space on every declared dataset's table"
        )
        maintain.add_argument("--config", default=None, help="datasets directory")
        maintain.add_argument("--stack-config", default=None, help="iceberg deployment directory")
        maintain.add_argument(
            "--branch", default=None, help="branch to optimize (default: the dataset's own)"
        )
        maintain.add_argument(
            "--dry-run", action="store_true", help="report what would be rewritten and expired"
        )
        maintain.add_argument(
            "--verbose", action="store_true", help="debug logging: every optimized detail"
        )
        maintain.add_argument(
            "--var",
            dest="variables",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="extra Jinja variable; repeatable",
        )
        maintain.set_defaults(run=self.optimize)

        listing = commands.add_parser("list", help="list declared datasets")
        listing.add_argument("--config", default=None, help="datasets directory")
        listing.set_defaults(run=self.list_datasets)

        whole = commands.add_parser("sync", help="rewrite every dataset file in full")
        whole.add_argument("--config", default=None, help="datasets directory")
        whole.add_argument(
            "--dry-run",
            action="store_true",
            help="report files that would change; exit 1 when any would",
        )
        whole.add_argument(
            "--var",
            dest="variables",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="extra Jinja variable; repeatable",
        )
        whole.set_defaults(run=self.sync)

    def deploy(self, arguments: argparse.Namespace) -> int:
        """Every declared dataset, autonomous: no per-table side file needed."""
        from rekep.dataset import Dataset

        level = logging.DEBUG if arguments.verbose else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s %(name)s %(message)s")
        context = _pairs(arguments.variables)
        datasets = Dataset.load_all(arguments.config, **context)

        for target in arguments.targets or ["iceberg"]:
            stack = self._stack(target, arguments.stack_config, context)
            verb = "would converge" if arguments.dry_run else "converged"
            for dataset in datasets:
                dataset.deploy(target, stack, dry_run=arguments.dry_run)
                print(f"{target} {verb}: {dataset.resource_uri()}")
        return 0

    def optimize(self, arguments: argparse.Namespace) -> int:
        """Every declared dataset's Iceberg table: compacted, then reclaimed.

        Idempotent like every other verb here: a table already laid out well
        reports nothing rewritten and nothing freed. The policy is each
        dataset's own (`protocols.iceberg.compact_min_files`, `retain`), so
        this is the whole command a scheduler needs.
        """
        from rekep.dataset import Dataset

        level = logging.DEBUG if arguments.verbose else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s %(name)s %(message)s")
        context = _pairs(arguments.variables)
        stack = self._stack("iceberg", arguments.stack_config, context)

        for dataset in Dataset.load_all(arguments.config, **context):
            table = stack.tables.get(dataset.into_iceberg_table())
            report = dataset.optimize(
                table=table, branch=arguments.branch, dry_run=arguments.dry_run
            )
            compaction, cleanup = report["compaction"], report["cleanup"]
            verb = "would rewrite" if arguments.dry_run else "rewrote"
            print(
                f"{dataset.resource_uri()}: {verb} {compaction['files']} files in "
                f"{len(compaction['partitions'])} partitions, "
                f"{len(cleanup['expired'])} snapshots expired, "
                f"{len(cleanup['orphans'])} files freed"
            )
        return 0

    def sync(self, arguments: argparse.Namespace) -> int:
        """Rewrite each dataset file with its record's schema written out.

        The same verb the stacks have, over the folder that now describes a
        data product on its own: `description` and `fields` come from the
        record, so the file a reviewer reads is the schema the code actually
        declares. `--dry-run` exits 1 when any file has drifted, which is how
        CI catches a model change that never reached its side file.
        """
        from rekep.dataset import Dataset

        folder = config.folder("datasets", arguments.config)
        drifted = _sync_folder(
            folder, Dataset, _pairs(arguments.variables), dry_run=arguments.dry_run
        )
        return 1 if arguments.dry_run and drifted else 0

    def list_datasets(self, arguments: argparse.Namespace) -> int:
        from rekep.dataset import Dataset

        for dataset in Dataset.load_all(arguments.config):
            print(f"{dataset.resource_uri()}  schema={dataset.schema}")
        return 0

    def _stack(self, target: str, config: str | None, context: dict[str, str]) -> Any:
        if target == "iceberg":
            from rekep.iceberg import Iceberg
            from rekep.records.iceberg import ICEBERG_ROOT

            return Iceberg.load(config or ICEBERG_ROOT, **context)
        from rekep.doris import Doris
        from rekep.records.doris import DORIS_ROOT

        return Doris.load(config or DORIS_ROOT, **context)


class DagService:
    """`rekep dag`: the dags this deployment declares, listed and run.

    A `rekep.dag.Dag` is rekep's own graph, not a view of an orchestrator's:
    the tasks it names, the order its `dependencies` imply, and a runner that
    walks that order in this process. `rekep airflow` projects the same dags
    onto Airflow; nothing here needs Airflow installed.
    """

    name = "dag"

    def register(self, commands: Any) -> None:
        parser = commands.add_parser(self.name, help="declared dags: list, show, run")
        commands = parser.add_subparsers(dest="command", required=True)

        listing = commands.add_parser("list", help="list declared dags")
        listing.add_argument("--config", default=None, help="dags directory")
        listing.add_argument("--jobs-config", default=None, help="jobs directory")
        listing.set_defaults(run=self.list_dags)

        show = commands.add_parser("show", help="one dag's tasks, in the order they run")
        show.add_argument(
            "--uri", required=True, help="dag uri, e.g. rekep:///dags/pipeline/trading_logs"
        )
        show.add_argument("--config", default=None, help="dags directory")
        show.add_argument("--jobs-config", default=None, help="jobs directory")
        show.set_defaults(run=self.show)

        runner = commands.add_parser("run", help="run every task, in dependency order")
        runner.add_argument(
            "--uri", required=True, help="dag uri, e.g. rekep:///dags/pipeline/trading_logs"
        )
        runner.add_argument("--config", default=None, help="dags directory")
        runner.add_argument("--jobs-config", default=None, help="jobs directory")
        runner.add_argument(
            "--verbose", action="store_true", help="debug logging: every running detail"
        )
        runner.set_defaults(run=self.run_dag)

    def list_dags(self, arguments: argparse.Namespace) -> int:
        from rekep.dag import load_all

        for dag in load_all(arguments.config):
            order = " -> ".join(dag.order(arguments.jobs_config)) or "no tasks"
            print(f"{dag.resource_uri()}  schedule={dag.schedule or '-'}  {order}")
        return 0

    def show(self, arguments: argparse.Namespace) -> int:
        """The graph, one task per line: what runs, and what it waited for."""
        from rekep.dag import find

        dag = find(arguments.uri, arguments.config)
        upstreams = dag.upstreams(arguments.jobs_config)
        print(f"{dag.resource_uri()}  schedule={dag.schedule or '-'}")
        for identifier in dag.order(arguments.jobs_config):
            task = dag.task(identifier, arguments.jobs_config)
            after = ", ".join(upstreams[identifier]) or "-"
            print(f"  {dag.task_name(task)}  uri={task.uri}  after={after}")
        return 0

    def run_dag(self, arguments: argparse.Namespace) -> int:
        from rekep.dag import find

        level = logging.DEBUG if arguments.verbose else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s %(name)s %(message)s")
        dag = find(arguments.uri, arguments.config)
        for identifier, result in dag.run(arguments.jobs_config).items():
            print(f"{dag.task_name(identifier)}: {result}")
        return 0


class AirflowService:
    """`rekep airflow`: DAG modules as a deployable resource."""

    name = "airflow"

    def register(self, commands: Any) -> None:
        parser = commands.add_parser(self.name, help="deployable Airflow DAG modules")
        commands = parser.add_subparsers(dest="command", required=True)

        deploy = commands.add_parser("deploy", help="converge DAG modules into a dags folder")
        deploy.add_argument("--config", default=None, help="dags directory")
        deploy.add_argument("--dags-folder", required=True, help="Airflow dags folder to write")
        deploy.add_argument(
            "--dry-run", action="store_true", help="log what would happen without writing"
        )
        deploy.add_argument(
            "--verbose", action="store_true", help="debug logging: every deployed detail"
        )
        deploy.set_defaults(run=self.deploy)

        dags = commands.add_parser("dags", help="the deployable module resource")
        verbs = dags.add_subparsers(dest="verb", required=True)
        listing = verbs.add_parser("list", help="list the dags that would be written")
        listing.add_argument("--config", default=None, help="dags directory")
        listing.set_defaults(run=self.list_dags)

    def deploy(self, arguments: argparse.Namespace) -> int:
        from rekep.airflow.service import Airflow

        level = logging.DEBUG if arguments.verbose else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s %(name)s %(message)s")
        deployed = Airflow.deploy_folder(
            arguments.config,
            dags_folder=arguments.dags_folder,
            dry_run=arguments.dry_run,
        )
        header = "would converge" if arguments.dry_run else "converged"
        print(f"{header} dags: {', '.join(deployed) or 'none'}")
        return 0

    def list_dags(self, arguments: argparse.Namespace) -> int:
        from rekep.airflow.service import Dags

        for dag in Dags(arguments.config).list():
            print(f"{dag.dag_id()}.py  {dag.resource_uri()}  {len(dag.tasks)} task(s)")
        return 0


SERVICES = (
    DdlService(),
    DocsService(),
    RecordsService(),
    DatasetService(),
    DagService(),
    IcebergService(),
    DorisService(),
    AirflowService(),
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="rekep",
        description=LOGO + "\n" + (__doc__ or ""),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    top = parser.add_subparsers(dest="command_group", required=True)
    for service in SERVICES:
        service.register(top)

    install = top.add_parser("install", help="stand doris or airflow up from nothing")
    targets = install.add_subparsers(dest="target", required=True)
    from rekep.install import INSTALLERS

    for installer in INSTALLERS.values():
        command = targets.add_parser(installer.name, help=f"install {installer.name}")
        command.add_argument(
            "--dry-run", action="store_true", help="show the exact commands without running"
        )
        command.set_defaults(run=_install, installer=installer)

    tutorial = top.add_parser("tutorial", help="a guided tour, zero to local lakehouse")
    tutorial.add_argument("--auto", action="store_true", help="run every step without prompting")
    tutorial.add_argument(
        "--workspace",
        default=None,
        type=pathlib.Path,
        help="where the tour builds (default: ./tutorial, gitignored)",
    )
    tutorial.set_defaults(run=_tutorial)

    arguments = parser.parse_args(argv)
    return arguments.run(arguments)


def _install(arguments: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")
    return 0 if arguments.installer.install(dry_run=arguments.dry_run) else 1


def _tutorial(arguments: argparse.Namespace) -> int:
    from rekep.tutorial import Tutorial

    return Tutorial(auto=arguments.auto, workspace=arguments.workspace).run()


def _sync_folder(
    folder: pathlib.Path,
    cls: type,
    context: dict[str, str],
    *,
    dry_run: bool = False,
    stem_names: bool = False,
) -> bool:
    """Rewrite every side file in `folder` in full; True when any drifted.

    One implementation for every folder of declarations, because they all
    want the same thing: load it, materialise what the record knows how to
    derive, write it back complete -- so a side file states its whole
    contract instead of only its overrides, and `--dry-run` says which ones
    no longer do.

    A file containing Jinja is left alone. Rewriting it would resolve the
    template against *this* machine's environment and bake the answer in,
    which is a worse outcome than a file this pass cannot keep current.

    `stem_names` is the registry-folder rule (`catalogs/`, `namespaces/`):
    the file stem supplies `name`. A dataset names itself in its own `uri`,
    so it does not want it.
    """
    import rekep.records.registry as registry

    drifted = False
    for path in sorted(folder.glob("*")) if folder.is_dir() else []:
        if path.suffix not in registry.EXTENSIONS:
            continue
        source = path.read_text(encoding="utf-8")
        if "{{" in source or "{%" in source:
            print(f"skipped {path} (templated)")
            continue
        mapping = registry.parse(path, context)
        if stem_names:
            mapping.setdefault("name", path.stem)
        entry = cls.from_dict(mapping)
        if hasattr(entry, "materialized"):
            entry = entry.materialized()
        fresh: bytes = getattr(entry, f"into_{cls.redirect_of(path.name)}")()
        if dry_run:
            if path.read_bytes().replace(b"\r\n", b"\n") != fresh:
                print(f"would rewrite {path}")
                drifted = True
            continue
        path.write_bytes(fresh)
        print(path)
    return drifted


def _record_class(reference: str) -> type[Record]:
    """The record `reference` names, or a refusal the shell can print.

    Every failure `Record.locate` raises is a user error here -- a name
    nobody declared, a name two records answer to, a class of the wrong kind
    -- so they arrive as `SystemExit` with the message intact rather than as
    a traceback.
    """
    try:
        return Record.locate(reference)
    except (KeyError, TypeError, ValueError) as error:
        # `str(KeyError)` is the *repr* of its argument, quotes and all; the
        # message itself is what a shell should print.
        raise SystemExit(error.args[0] if error.args else str(error)) from error


def _pairs(items: Sequence[str]) -> dict[str, str]:
    """`["k=v", ...]` -> `{"k": "v", ...}`, refusing entries with no `=`."""
    pairs = {}
    for item in items:
        key, separator, value = item.partition("=")
        if not separator:
            raise SystemExit(f"expected KEY=VALUE, got {item!r}")
        pairs[key] = value
    return pairs


if __name__ == "__main__":
    raise SystemExit(main())
