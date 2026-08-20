"""`rekep tutorial`: a guided, animated tour from zero to a local lakehouse.

Every step is real -- the record is projected, the DDL rendered, the local
catalog deployed, the sample log parsed -- inside a scratch folder, so the
adventure leaves nothing behind but understanding. Rich (already a core
dependency) does the display; `auto=True` runs straight through for scripts
and CI.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
from rich.syntax import Syntax

SAMPLE = (
    "2026-08-14 00:05:01.147_250 [250-e725:72505] [OMSSales_Enrichment] (DEBUG) CLIENTID set\n"
    "2026-08-14 00:05:01.147_514 [250-e725:72505] [ULBridge] (INFO) Message received\n"
    "2026-08-14 00:05:01.148_339 [250-e725:72504] [ObjkeyTagWrapper] (ERROR) Expression raised\n"
    "java.lang.IllegalStateException: no binding for token\n"
    "2026-08-14 00:05:02.001_007 [250-e725:72505] [ULBridge] (INFO) Message type {cancel}\n"
)

DECLARATION = '''@record
class Log(Record):
    """One parsed line of a trading log."""

    url: str
    """Path of the log the line came from."""

    unix: int
    """Timestamp as whole nanoseconds since the epoch, naive UTC."""

    date: Annotated[datetime.date, Arrow(partition=True)]
    """Calendar day of the timestamp -- the lake partitions on it."""
'''


class Tutorial:
    """The steps, in the order a new deployment meets them."""

    def __init__(
        self,
        auto: bool = False,
        console: Console | None = None,
        workspace: Path | None = None,
    ) -> None:
        self.auto = auto
        # A cp1252 stdout cannot carry the emoji; UTF-8 always can, and
        # `errors="replace"` keeps even a stranger encoding from crashing.
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
        except (AttributeError, OSError):  # pragma: no cover - exotic streams
            pass
        # legacy_windows off: the VT path works on every modern terminal, and
        # the legacy Win32 renderer crashes when stdout is a pipe.
        self.console = console or Console(legacy_windows=False)
        # The workspace persists -- gitignored `tutorial/` at the repo root --
        # so everything the tour builds can be inspected afterwards. Resolved
        # absolute: the catalog stores these paths, and a relative one breaks
        # the moment the next run starts from another directory.
        self.scratch = (workspace or Path("tutorial")).resolve()

    def run(self) -> int:
        steps = (
            self.welcome,
            self.declare,
            self.project,
            self.render_ddl,
            self.deploy_local,
            self.parse,
            self.doris,
            self.airflow,
            self.next_steps,
        )
        self.scratch.mkdir(parents=True, exist_ok=True)
        for index, step in enumerate(steps, start=1):
            if not self._proceed(index, len(steps)):
                self.console.print("[dim]Stopped -- come back any time.[/dim]")
                return 0
            step()
        return 0

    # -- steps --------------------------------------------------------------

    def welcome(self) -> None:
        from rekep.cli import LOGO

        self.console.print(f"[bold cyan]{LOGO}[/bold cyan]")
        self.panel(
            "Welcome to rekep",
            "One dataclass is the whole data product: schema, files, DDL,\n"
            "tables, lineage. This tour builds a **fully local lakehouse** --\n"
            "no services, nothing installed, nothing left behind.",
            emoji=":compass:",
        )

    def declare(self) -> None:
        self.panel(
            "1 · Declare a record",
            "A field's docstring becomes its column comment everywhere.\n"
            "`Arrow(partition=True)` on `date` partitions every table below.",
        )
        self.console.print(Syntax(DECLARATION, "python", background_color="default"))

    def project(self) -> None:
        from rekep.models import Log

        self.panel("2 · Project it", "The same declaration, asked in Arrow terms:")
        with self.spin("projecting"):
            schema = Log.into_arrow_schema()
        for field in schema:
            metadata = field.metadata or {}
            self.console.print(
                f"  [cyan]{field.name:12}[/cyan] [magenta]{field.type!s:12}[/magenta] "
                f"[dim]{(metadata.get(b'description') or b'').decode()[:56]}[/dim]"
            )

    def render_ddl(self) -> None:
        from rekep.models import Log

        self.panel("3 · Render DDL", "Iceberg first; `--dialect doris` is the same one call.")
        with self.spin("rendering"):
            ddl = Log.into_iceberg_ddl("demo.logs")
        self.console.print(Syntax(ddl, "sql", background_color="default"))

    def deploy_local(self) -> None:
        from rekep.iceberg import Iceberg
        from rekep.records.iceberg import IcebergCatalog, IcebergDeployment, IcebergTable

        self.panel(
            "4 · Deploy, locally",
            "A SQLite catalog and a file warehouse -- the same `deploy` that\n"
            "converges production, running in a scratch folder:",
        )
        root = self.scratch.as_posix()
        stack = Iceberg(
            IcebergDeployment(
                catalogs=[
                    IcebergCatalog(
                        uri=f"sqlite:///{root}/catalog.db", warehouse=f"file://{root}/wh"
                    )
                ],
                tables=[IcebergTable(record="rekep.models.Log", name="logs")],
            )
        )
        with self.progress(("catalog", "namespace", "table")) as advance:
            stack.catalogs.check("iceberg")
            advance()
            stack.namespaces.get_or_create(stack.namespaces.list()[0])
            advance()
            stack.tables.create_or_update(stack.tables.list()[0])
            advance()
        self.console.print("  [green]:heavy_check_mark:[/green] iceberg.default.logs is live\n")

    def parse(self) -> None:
        from rekep.logs import LogFile

        self.panel(
            "5 · Parse a log",
            "Streaming, Arrow-native: regex on bytes, timestamps converted\n"
            "per batch by Arrow compute, stack traces folded into their row.",
        )
        sample = self.scratch / "app.txt"
        sample.write_text(SAMPLE, encoding="utf-8")
        with self.spin("parsing"), LogFile.from_path(sample) as log:
            table = log.into_arrow_table()
        self.console.print(
            f"  [green]{table.num_rows} rows[/green] from {len(SAMPLE.splitlines())} lines "
            f"[dim](the stack trace folded into its ERROR row)[/dim]\n"
        )

    def doris(self) -> None:
        from rekep.doris import Doris

        self.panel(
            "6 \u00b7 The same table, in Doris",
            "One declaration, another engine: the plan is ordered\n"
            "catalog -> database -> table, keys lead, the date partitions.",
        )
        with self.spin("planning"):
            statements = Doris.deploy_folder(self.scratch / "nowhere", dry_run=True)
        for statement in statements:
            self.console.print(Syntax(statement, "sql", background_color="default"))
        self.offer_install("doris")

    def airflow(self) -> None:
        from rekep.airflow.lineage import documentation_of
        from rekep.models import Log

        self.panel(
            "7 \u00b7 Orchestrate with Airflow",
            "A `Job` is an OpenLineage resource plus one `arrow_transform`; a\n"
            "side file in `stacks/jobs/` becomes a DAG whose lineage draws itself:",
        )
        declared = (
            "job: rekep.job.Passthrough\n"
            "name: passthrough\n"
            'schedule: "@daily"\n'
            "consumes: [rekep.models.Log]\n"
            "produces: [rekep.models.Log]\n"
        )
        self.console.print(Syntax(declared, "yaml", background_color="default"))
        with self.spin("deriving lineage"):
            lineage = documentation_of([Log], [Log])
        self.console.print(Markdown(lineage))
        self.offer_install("airflow")

    def next_steps(self) -> None:
        self.panel(
            "Where next",
            "- `stacks/iceberg/`, `stacks/doris/` -- declare your deployment\n"
            "- `rekep iceberg deploy --dry-run` -- see a real plan\n"
            "- `rekep install doris` / `rekep install airflow` -- go live\n"
            "- docs: Use cases → Tutorial for the full written version",
            emoji=":rocket:",
        )

    def offer_install(self, name: str) -> None:
        """Show the service's state and, interactively, offer to install it.

        `--auto` never installs -- an unattended run must not pull images or
        packages -- and a blocked installer explains itself instead of asking.
        """
        from rekep.install import INSTALLERS

        installer = INSTALLERS[name]
        links = "  ".join(f"{label} [link]{url}[/link]" for label, url in installer.urls().items())
        if installer.installed():
            self.console.print(f"  [green]:heavy_check_mark:[/green] {name} is up -- {links}\n")
            return
        blockers = installer.requirements()
        if blockers:
            self.console.print(f"  [yellow]no {name} here[/yellow], and it cannot install yet:")
            for blocker in blockers:
                self.console.print(f"    [dim]{blocker}[/dim]")
            self.console.print()
            return
        if self.auto:
            self.console.print(
                f"  [yellow]no {name} here[/yellow] -- `rekep install {name}` stands it up, "
                f"then {links}\n"
            )
            return
        if not Confirm.ask(f"  no {name} here -- install it now?", default=False):
            self.console.print(f"  [dim]skipped; `rekep install {name}` any time.[/dim]\n")
            return
        with self.spin(f"installing {name}"):
            done = installer.install()
        if done:
            self.console.print(f"  [green]:heavy_check_mark:[/green] {name} installed -- {links}\n")
        else:
            self.console.print(
                f"  [red]{name} install did not converge; see the log above.[/red]\n"
            )

    # -- display ------------------------------------------------------------

    def panel(self, title: str, body: str, emoji: str = ":gear:") -> None:
        self.console.print(Panel(Markdown(body), title=f"{emoji} [bold]{title}[/bold]", width=76))

    def spin(self, label: str):
        return self.console.status(f"[cyan]{label}...", spinner="dots")

    def progress(self, levels: tuple[str, ...]):
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console,
        )

        class Stepper:
            def __enter__(inner):
                progress.start()
                inner.task = progress.add_task(f"deploying {levels[0]}", total=len(levels))
                inner.index = 0
                return inner.advance

            def advance(inner) -> None:
                inner.index += 1
                time.sleep(0.15 if not self.auto else 0)  # let the motion register
                description = (
                    f"deploying {levels[inner.index]}" if inner.index < len(levels) else "done"
                )
                progress.update(inner.task, advance=1, description=description)

            def __exit__(inner, *exc) -> None:
                progress.stop()

        return Stepper()

    def _proceed(self, index: int, total: int) -> bool:
        if self.auto or index == 1:
            return True
        return Confirm.ask(f"[dim]step {index}/{total} --[/dim] continue?", default=True)
