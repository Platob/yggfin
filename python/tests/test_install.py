"""Installers: honest checks, exact plans, idempotent installs."""

import logging

import pytest

from rekep.install import INSTALLERS, AirflowInstaller, DorisInstaller


def test_the_registry_names_all_three() -> None:
    assert set(INSTALLERS) == {"docker", "doris", "airflow"}


def test_installed_is_a_noop(caplog: pytest.LogCaptureFixture) -> None:
    installer = DorisInstaller()
    installer.installed = lambda: True  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="rekep.install"):
        assert installer.install() is True
    assert "already installed, nothing to do" in caplog.text


def test_blockers_stop_the_install(caplog: pytest.LogCaptureFixture) -> None:
    installer = DorisInstaller()
    installer.installed = lambda: False  # type: ignore[method-assign]
    installer.dependencies = lambda: []  # type: ignore[method-assign]
    installer.requirements = lambda: ["docker is required"]  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="rekep.install"):
        assert installer.install() is False
    assert "docker is required" in caplog.text


def test_dry_run_logs_the_exact_commands(caplog: pytest.LogCaptureFixture) -> None:
    installer = DorisInstaller()
    installer.installed = lambda: False  # type: ignore[method-assign]
    installer.dependencies = lambda: []  # type: ignore[method-assign]
    installer.requirements = lambda: []  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="rekep.install"):
        assert installer.install(dry_run=True) is True
    assert "would run docker network create rekep-doris" in caplog.text
    assert f"apache/doris:fe-{installer.version}" in caplog.text


def test_doris_plan_is_fe_then_be() -> None:
    plan = DorisInstaller().plan()
    assert [argv[0] for argv in plan] == ["docker", "docker", "docker"]
    assert "rekep-doris-fe" in plan[1]
    assert "rekep-doris-be" in plan[2]


def test_airflow_check_asks_python_not_the_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib.util

    installer = AirflowInstaller()
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
    assert installer.installed() is False
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    assert installer.installed() is True
    assert ["uv", "pip", "install", "rekep[airflow]"] in installer.plan()


def test_urls_are_shown_when_already_installed(caplog: pytest.LogCaptureFixture) -> None:
    installer = DorisInstaller()
    installer.installed = lambda: True  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="rekep.install"):
        installer.install()
    assert "web ui at http://localhost:8030" in caplog.text
    assert "mysql at mysql://root@localhost:9030" in caplog.text


def test_urls_are_prospective_on_dry_run(caplog: pytest.LogCaptureFixture) -> None:
    installer = DorisInstaller()
    installer.installed = lambda: False  # type: ignore[method-assign]
    installer.dependencies = lambda: []  # type: ignore[method-assign]
    installer.requirements = lambda: []  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="rekep.install"):
        installer.install(dry_run=True)
    assert "web ui will be at http://localhost:8030" in caplog.text


def test_airflow_urls() -> None:
    assert AirflowInstaller().urls() == {"web ui": "http://localhost:8080"}


# -- dependency convergence --------------------------------------------------


def test_doris_depends_on_docker() -> None:
    from rekep.install import DockerInstaller

    (dependency,) = DorisInstaller().dependencies()
    assert isinstance(dependency, DockerInstaller)


def test_dependency_converges_before_the_plan(caplog: pytest.LogCaptureFixture) -> None:
    installer = DorisInstaller()
    installer.installed = lambda: False  # type: ignore[method-assign]
    installer.requirements = lambda: []  # type: ignore[method-assign]

    class StubDocker:
        name = "docker"
        calls: list[bool] = []

        def install(self, dry_run: bool = False) -> bool:
            self.calls.append(dry_run)
            return True

    stub = StubDocker()
    installer.dependencies = lambda: [stub]  # type: ignore[method-assign]
    with caplog.at_level(logging.INFO, logger="rekep.install"):
        assert installer.install(dry_run=True) is True
    assert stub.calls == [True], "the dependency saw the same dry_run"
    assert "would run docker network create" in caplog.text


def test_a_failed_dependency_stops_the_install(caplog: pytest.LogCaptureFixture) -> None:
    installer = DorisInstaller()
    installer.installed = lambda: False  # type: ignore[method-assign]

    class Broken:
        name = "docker"

        def install(self, dry_run: bool = False) -> bool:
            return False

    installer.dependencies = lambda: [Broken()]  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR, logger="rekep.install"):
        assert installer.install() is False
    assert "dependency docker did not converge" in caplog.text


def test_docker_install_step_matches_the_platform() -> None:
    from rekep.install import DockerInstaller

    installer = DockerInstaller()
    assert installer.install_step("Windows")[:4] == ["winget", "install", "-e", "--id"]
    assert installer.install_step("Darwin") == ["brew", "install", "--cask", "docker"]
    assert installer.install_step("Linux") == ["sh", "-c", "curl -fsSL https://get.docker.com | sh"]


def test_docker_start_step_matches_the_platform() -> None:
    from rekep.install import DockerInstaller

    installer = DockerInstaller()
    assert installer.start_step("Darwin") == ["open", "-a", "Docker"]
    assert installer.start_step("Linux") == ["sudo", "systemctl", "start", "docker"]
    assert installer.start_step("Windows")[0] == "cmd"


def test_docker_plan_skips_the_install_when_the_binary_is_there(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A present binary with a dead daemon only needs starting."""
    import shutil

    from rekep.install import DockerInstaller

    installer = DockerInstaller()
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/docker")
    assert len(installer.plan()) == 1, "start only"
    monkeypatch.setattr(shutil, "which", lambda name: None)
    assert len(installer.plan()) == 2, "install then start"


def test_docker_installed_means_the_daemon_answers(monkeypatch: pytest.MonkeyPatch) -> None:
    import shutil

    from rekep.install import DockerInstaller

    installer = DockerInstaller()
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/docker")
    monkeypatch.setattr(installer, "daemon_running", lambda: False)
    assert installer.installed() is False, "a binary without a daemon is not usable"
    monkeypatch.setattr(installer, "daemon_running", lambda: True)
    assert installer.installed() is True


def test_settle_waits_for_the_daemon(monkeypatch: pytest.MonkeyPatch) -> None:
    import time as time_module

    from rekep.install import DockerInstaller

    installer = DockerInstaller()
    answers = iter([False, False, True])
    monkeypatch.setattr(installer, "daemon_running", lambda: next(answers))
    monkeypatch.setattr(time_module, "sleep", lambda seconds: None)
    installer.settle()  # returns as soon as the daemon answers


def test_doris_has_no_blockers_of_its_own() -> None:
    """The daemon is the docker dependency's problem, not a dead end here."""
    assert DorisInstaller().requirements() == []
