"""What the library did, for whoever reads the run afterwards.

`Console` renders for the person who just typed a command; this records what
the library did for the operator reading a task log an hour later. One fact
belongs to exactly one of them.

Levels are the whole policy, so they are stated once, here:

- **INFO** is a completed operation: a commit landed, a table was created, a
  maintenance pass settled. One record per public verb, whatever it did
  inside -- a write that commits forty chunks is one INFO, not forty.
- **DEBUG** is the detail under it: a cast, a projection, a scan plan, a file
  opened, a registry index built. Per stream and per file, never per batch or
  per row.

Nothing here runs at import. A library that configures logging has decided for
its caller, and with no handler installed the standard library's last resort
already carries WARNING and above to `stderr` -- so an application that never
calls `configure` sees exactly what it saw before this module existed.
"""

from __future__ import annotations

import logging
import sys

#: The parent of every logger in the package. Each module holds its own
#: `logging.getLogger(__name__)`, so a record says which module emitted it and
#: a grep for that name reaches the emitting line; this is what an operator
#: filters or silences the whole package by.
ROOT = "rekep"

#: What a task run shows when nothing says otherwise. A notebook is read after
#: the fact by somebody asking what happened, which is what INFO answers.
TASK_LEVEL = "INFO"

#: What the CLI shows when nothing says otherwise. A person at a terminal is
#: reading `Console`, and a second stream of records over it is noise.
COMMAND_LEVEL = "WARNING"

#: Level, logger, message. No colour: `Console` owns escape sequences, and a
#: log stream is read by `grep` at least as often as by an eye.
FORMAT = "%(levelname)s %(name)s %(message)s"


def configure(level: str | int = TASK_LEVEL) -> logging.Logger:
    """Send this package's records to `stderr` at `level`, once.

    Idempotent, and scoped to `rekep` rather than the root logger: configuring
    the root would decide the level for every library the caller also imports.
    Repeated calls move the level and leave one handler, so a notebook that
    runs two tasks does not print every record twice.
    """
    logger = logging.getLogger(ROOT)
    logger.setLevel(logging.getLevelNamesMapping()[level] if isinstance(level, str) else level)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    # Resolved now rather than held: `sys.stderr` under papermill is the
    # kernel's, and a handler built at import would hold whatever it was then.
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)
    # The records are this package's own; passing them up would print them a
    # second time wherever the application has configured a root handler.
    logger.propagate = False
    return logger
