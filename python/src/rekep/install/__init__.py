"""Installers: stand up the stacks rekep talks to, from nothing.

Each installer follows the same contract: `installed()` is an honest check
(never a guess), `plan()` is the exact commands that would run, and
`install()` runs them -- or just shows them with `dry_run`. Nothing here
re-installs over a live service: `install` is `get_or_create` for
infrastructure.
"""

from __future__ import annotations

import importlib.util
import logging
import platform
import shutil
import socket
import subprocess
import time
from typing import ClassVar

__all__ = ["AirflowInstaller", "DockerInstaller", "DorisInstaller", "INSTALLERS", "Installer"]

logger = logging.getLogger("rekep.install")


class Installer:
    """One service: check, plan, install -- idempotent end to end."""

    name: ClassVar[str] = ""

    def installed(self) -> bool:
        raise NotImplementedError

    def plan(self) -> list[list[str]]:
        """The exact commands `install` would run, argv by argv."""
        raise NotImplementedError

    def requirements(self) -> list[str]:
        """Human-readable blockers; empty when installation can proceed."""
        return []

    def urls(self) -> dict[str, str]:
        """Where the service shows itself once it is up, by label."""
        return {}

    def dependencies(self) -> list[Installer]:
        """Installers that must converge before this one's own plan runs."""
        return []

    def install(self, dry_run: bool = False) -> bool:
        """Converge to installed; True when the service is present after.

        Already installed is success and a no-op -- logged as such, exactly
        like the deploy verbs.
        """
        if self.installed():
            logger.info("%s: already installed, nothing to do", self.name)
            self._show_urls()
            return True
        for dependency in self.dependencies():
            if not dependency.install(dry_run=dry_run):
                logger.error("%s: dependency %s did not converge", self.name, dependency.name)
                return False
        blockers = self.requirements()
        if blockers:
            for blocker in blockers:
                logger.error("%s: %s", self.name, blocker)
            return False
        for argv in self.plan():
            logger.info("%s: %s %s", self.name, "would run" if dry_run else "run", " ".join(argv))
            if not dry_run:
                subprocess.run(argv, check=True)  # noqa: S603 - fixed argv, no shell
        if not dry_run:
            self.settle()
        self._show_urls(prospective=dry_run)
        return dry_run or self.installed()

    def settle(self) -> None:
        """Work after the plan that is not a command -- waiting, mostly."""
        return None

    def _show_urls(self, prospective: bool = False) -> None:
        for label, url in self.urls().items():
            verb = "will be at" if prospective else "at"
            logger.info("%s: %s %s %s", self.name, label, verb, url)


class DockerInstaller(Installer):
    """Docker itself: installed *and* running, because that is what "usable" means.

    Anything downstream needs a daemon that answers, not a binary on PATH, so
    `installed` probes `docker info` and the plan does whatever is missing --
    install per platform (winget, brew cask, Docker's convenience script),
    then start (Docker Desktop, or systemd on Linux) -- and `settle` waits for
    the daemon to actually come up, which takes a while on Desktop.
    """

    name = "docker"

    #: How long to wait for the daemon after starting it, in seconds.
    STARTUP_TIMEOUT = 120

    def installed(self) -> bool:
        """True when a daemon answers -- the only honest definition."""
        return shutil.which("docker") is not None and self.daemon_running()

    def daemon_running(self) -> bool:
        try:
            probe = subprocess.run(  # noqa: S603, S607 - fixed argv, no shell
                ["docker", "info"], capture_output=True, timeout=15, check=False
            )
        except (OSError, subprocess.TimeoutExpired):
            return False
        return probe.returncode == 0

    def settle(self) -> None:
        """Wait for the daemon; Docker Desktop can take a minute to be ready."""
        deadline = time.monotonic() + self.STARTUP_TIMEOUT
        while time.monotonic() < deadline:
            if self.daemon_running():
                logger.info("%s: daemon is up", self.name)
                return
            time.sleep(3)
        logger.warning(
            "%s: daemon did not answer within %ds; it may still be starting",
            self.name,
            self.STARTUP_TIMEOUT,
        )

    def requirements(self) -> list[str]:
        system = platform.system()
        if system == "Windows" and shutil.which("winget") is None:
            return ["winget is required to install Docker Desktop on Windows"]
        if system == "Darwin" and shutil.which("brew") is None:
            return ["homebrew is required to install Docker Desktop on macOS"]
        if system == "Linux" and shutil.which("curl") is None:
            return ["curl is required to fetch https://get.docker.com"]
        return []

    def plan(self) -> list[list[str]]:
        """Install when the binary is missing, then start the daemon.

        A present binary with a dead daemon skips straight to starting, so
        this converges either state without reinstalling anything.
        """
        system = platform.system()
        steps: list[list[str]] = []
        if shutil.which("docker") is None:
            steps.append(self.install_step(system))
        steps.append(self.start_step(system))
        return steps

    def install_step(self, system: str) -> list[str]:
        if system == "Windows":
            return [
                "winget",
                "install",
                "-e",
                "--id",
                "Docker.DockerDesktop",
                "--accept-source-agreements",
                "--accept-package-agreements",
            ]
        if system == "Darwin":
            return ["brew", "install", "--cask", "docker"]
        return ["sh", "-c", "curl -fsSL https://get.docker.com | sh"]

    def start_step(self, system: str) -> list[str]:
        if system == "Windows":
            return ["cmd", "/c", "start", "", "Docker Desktop.exe"]
        if system == "Darwin":
            return ["open", "-a", "Docker"]
        return ["sudo", "systemctl", "start", "docker"]


class DorisInstaller(Installer):
    """Apache Doris via its official docker images (`apache/doris`).

    Doris publishes FE/BE runtime images from 2.1.8 on; one of each is
    enough for a local lakehouse. `installed` probes the FE MySQL port
    rather than trusting container names -- reachable is the only honest
    definition of installed.
    """

    name = "doris"

    #: FE MySQL-protocol port; reachable means a live cluster.
    PORT = 9030

    #: Image version; FE/BE tags follow `fe-<version>` / `be-<version>`.
    version = "3.0.5"

    def urls(self) -> dict[str, str]:
        return {
            "web ui": "http://localhost:8030",
            "mysql": "mysql://root@localhost:9030",
        }

    def installed(self) -> bool:
        try:
            with socket.create_connection(("127.0.0.1", self.PORT), timeout=1):
                return True
        except OSError:
            return False

    def dependencies(self) -> list[Installer]:
        return [DockerInstaller()]

    def requirements(self) -> list[str]:
        """Nothing of its own: the docker dependency installs and starts it."""
        return []

    def plan(self) -> list[list[str]]:
        fe, be = f"apache/doris:fe-{self.version}", f"apache/doris:be-{self.version}"
        return [
            ["docker", "network", "create", "rekep-doris"],
            [
                "docker",
                "run",
                "-d",
                "--name",
                "rekep-doris-fe",
                "--network",
                "rekep-doris",
                "-p",
                "8030:8030",
                "-p",
                "9030:9030",
                "-e",
                "FE_SERVERS=fe1:rekep-doris-fe:9010",
                "-e",
                "FE_ID=1",
                fe,
            ],
            [
                "docker",
                "run",
                "-d",
                "--name",
                "rekep-doris-be",
                "--network",
                "rekep-doris",
                "-p",
                "8040:8040",
                "-e",
                "FE_SERVERS=fe1:rekep-doris-fe:9010",
                "-e",
                "BE_ADDR=rekep-doris-be:9050",
                be,
            ],
        ]


class AirflowInstaller(Installer):
    """Apache Airflow into this environment, via the `airflow` extra.

    `installed` asks Python, not the shell: the package being importable is
    what `rekep.airflow` actually needs. Airflow itself is POSIX-only, so on
    Windows the requirement names WSL2 instead of pretending.
    """

    name = "airflow"

    def installed(self) -> bool:
        return importlib.util.find_spec("airflow") is not None

    def requirements(self) -> list[str]:
        if platform.system() == "Windows":
            return ["airflow is POSIX-only; install inside WSL2 or a container"]
        return []

    def urls(self) -> dict[str, str]:
        return {"web ui": "http://localhost:8080"}

    def plan(self) -> list[list[str]]:
        return [
            ["uv", "pip", "install", "rekep[airflow]"],
            ["airflow", "standalone"],
        ]


INSTALLERS = {
    installer.name: installer
    for installer in (DockerInstaller(), DorisInstaller(), AirflowInstaller())
}
