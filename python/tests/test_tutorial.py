"""The guided tour must actually run, end to end, unattended."""

from pathlib import Path

from rich.console import Console

from rekep.tutorial import Tutorial


def test_auto_mode_runs_every_step(tmp_path: Path) -> None:
    console = Console(record=True, width=80, legacy_windows=False)
    assert Tutorial(auto=True, console=console, workspace=tmp_path).run() == 0
    text = console.export_text()
    assert "Welcome to rekep" in text
    assert "CREATE TABLE IF NOT EXISTS demo.logs" in text
    assert "is live" in text
    assert "4 rows" in text, "the sample parses and the stack trace folds"
    assert "The same table, in Doris" in text
    assert "CREATE DATABASE IF NOT EXISTS" in text, "the doris plan renders"
    assert "Orchestrate with Airflow" in text
    assert "Consumes" in text, "the lineage table renders"
    assert "Where next" in text


def test_offer_install_asks_and_installs(monkeypatch, tmp_path: Path) -> None:
    """When unblocked and interactive, accepting the prompt runs the installer."""
    import rekep.tutorial as tutorial_module
    from rekep.install import INSTALLERS

    console = Console(record=True, width=80, legacy_windows=False)
    tour = Tutorial(auto=False, console=console, workspace=tmp_path)

    installer = INSTALLERS["doris"]
    calls: list[str] = []
    monkeypatch.setattr(installer, "installed", lambda: False)
    monkeypatch.setattr(installer, "requirements", lambda: [])
    monkeypatch.setattr(installer, "install", lambda dry_run=False: calls.append("install") or True)
    monkeypatch.setattr(tutorial_module.Confirm, "ask", staticmethod(lambda *a, **k: True))

    tour.offer_install("doris")
    assert calls == ["install"]
    assert "doris installed" in console.export_text()


def test_offer_install_declined_is_a_hint(monkeypatch, tmp_path: Path) -> None:
    import rekep.tutorial as tutorial_module
    from rekep.install import INSTALLERS

    console = Console(record=True, width=80, legacy_windows=False)
    tour = Tutorial(auto=False, console=console, workspace=tmp_path)
    installer = INSTALLERS["airflow"]
    monkeypatch.setattr(installer, "installed", lambda: False)
    monkeypatch.setattr(installer, "requirements", lambda: [])
    monkeypatch.setattr(tutorial_module.Confirm, "ask", staticmethod(lambda *a, **k: False))

    tour.offer_install("airflow")
    assert "rekep install airflow" in console.export_text()


def test_auto_mode_never_installs(monkeypatch, tmp_path: Path) -> None:
    from rekep.install import INSTALLERS

    console = Console(record=True, width=80, legacy_windows=False)
    tour = Tutorial(auto=True, console=console, workspace=tmp_path)
    installer = INSTALLERS["doris"]
    monkeypatch.setattr(installer, "installed", lambda: False)
    monkeypatch.setattr(installer, "requirements", lambda: [])
    monkeypatch.setattr(
        installer, "install", lambda dry_run=False: (_ for _ in ()).throw(AssertionError)
    )

    tour.offer_install("doris")
    assert "stands it up" in console.export_text()
