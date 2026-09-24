"""Compact tables, retain recent snapshots, and sweep unreachable files."""

from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import Any

from rekep.iceberg import IcebergCatalog
from rekep.logs import Stage, configure

#: Maintenance rewrites the tables it visits and declares none of its own.
TARGETS: tuple[str, ...] = ()


def run(
    *,
    catalog: Mapping[str, Any],
    namespace: str | None,
    branch: str,
    min_files: int,
    retain: int,
    snapshot_age_days: float | None,
    orphan_age_days: float,
    remove_orphans: bool,
    metadata: bool,
    log_level: str,
) -> dict[str, Any]:
    """Optimize every table of `namespace`, or of the whole catalog when null."""
    # Records go to stderr from here on.
    configure(log_level)
    if type(retain) is not int or retain < 1:
        raise ValueError("retain must be a positive integer")
    if type(min_files) is not int or min_files < 2:
        raise ValueError("min_files must be at least 2")
    if snapshot_age_days is not None and snapshot_age_days < 0:
        raise ValueError("snapshot_age_days must be non-negative or null")
    if orphan_age_days < 0:
        raise ValueError("orphan_age_days must be non-negative")
    snapshot_age = None if snapshot_age_days is None else datetime.timedelta(days=snapshot_age_days)
    orphan_age = datetime.timedelta(days=orphan_age_days)
    store = IcebergCatalog.from_dict(catalog)
    try:
        # The catalog is open before the run is announced, so the stage names
        # it from the handle rather than from the parameter.
        stage = Stage("optimize_iceberg", sources={"catalog": store.name})
        reports = {
            dataset.identifier: dataset.optimize(
                branch=branch,
                min_files=min_files,
                retain=retain,
                older_than=snapshot_age,
                remove_orphans=remove_orphans,
                orphan_age=orphan_age,
                metadata=metadata,
            )
            for dataset in store.datasets(namespace)
        }
    finally:
        store.close()
    rewritten = sum(report["rewritten"] for report in reports.values())
    deleted = sum(report["deleted"] for report in reports.values())
    byte_size = sum(report["bytes"] for report in reports.values())
    expired = sum(report["expired"] for report in reports.values())
    stage.says(
        "visited %d tables: %d parts compacted, %d snapshots expired, %d files swept (%d bytes)",
        len(reports),
        rewritten,
        expired,
        deleted,
        byte_size,
    )
    # A maintenance pass reads every table it visits and writes the parts it
    # compacted, which is what `read` and `written` mean everywhere else.
    return stage.finished(
        read=len(reports),
        written=rewritten,
        skipped=len(reports) - sum(1 for report in reports.values() if report["rewritten"]),
        tables=len(reports),
        expired=expired,
        deleted=deleted,
        byte_size=byte_size,
        reports=reports,
    )
