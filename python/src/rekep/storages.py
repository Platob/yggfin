"""The three Iceberg catalogs a run lands its tables in: bronze, silver and gold.

A table is named `<layer>.<namespace>.<table>` -- `bronze.record_keeping.log_messages`
-- and the layer is the catalog that holds it. Bronze holds what was read
as it was read: the captured lines and every FIX frame parsed out of them.
Silver holds what a walk settled: one row per FIX event, the books folded
from them, and the order, quote and execution events flattened out of the
books. Gold is the consumers' layer: aggregates built from silver, which no
stage here writes.

The catalogs are the caller's, and so is where they live: three SQLite
catalogs on one machine, three Glue databases, three S3 Tables buckets. A
stage opens the datasets it reads and writes through them and closes those,
never the catalogs.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator, Mapping
from typing import Any

from rekep.annotations import Self
from rekep.iceberg import IcebergCatalog, IcebergDataset

#: The layers, in the order data moves through them.
LAYERS = ("bronze", "silver", "gold")


@dataclasses.dataclass(eq=False)
class Storages:
    """One Iceberg catalog per layer, and the tables addressed across them."""

    bronze: IcebergCatalog
    silver: IcebergCatalog
    gold: IcebergCatalog

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Mapping[str, Any]]) -> Self:
        """Build from one `IcebergCatalog.from_dict` mapping per layer.

        Every layer is required and nothing else is read: a missing layer, or
        a key naming none, is refused before a catalog is opened.
        """
        missing = [layer for layer in LAYERS if layer not in mapping]
        unknown = sorted(set(mapping) - set(LAYERS))
        if missing or unknown:
            said = [f"missing {', '.join(missing)}" if missing else ""]
            said += [f"unknown {', '.join(unknown)}" if unknown else ""]
            raise ValueError(
                f"expected one catalog per layer ({', '.join(LAYERS)}): "
                + "; ".join(part for part in said if part)
            )
        return cls(**{layer: IcebergCatalog.from_dict(mapping[layer]) for layer in LAYERS})

    def catalog(self, layer: str) -> IcebergCatalog:
        """The catalog one layer names."""
        if layer not in LAYERS:
            raise ValueError(f"expected a layer of {', '.join(LAYERS)}, got {layer!r}")
        catalog: IcebergCatalog = getattr(self, layer)
        return catalog

    def dataset(self, identifier: str, **kwargs: Any) -> IcebergDataset:
        """The dataset `<layer>.<namespace>.<table>` names, open or to be created.

        `kwargs` are `IcebergCatalog.dataset`'s: the declared `field`,
        `merge_schema`, `table_properties`, `branch` and the rest.
        """
        layer, table = split_identifier(identifier)
        return self.catalog(layer).dataset(table, **kwargs)

    def tables(self) -> Iterator[str]:
        """Every table the three catalogs hold, as `<layer>.<namespace>.<table>`."""
        for layer in LAYERS:
            for table in self.catalog(layer).tables():
                yield f"{layer}.{table}"

    def close(self) -> None:
        """Close the three catalogs."""
        for layer in LAYERS:
            self.catalog(layer).close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def split_identifier(identifier: str) -> tuple[str, str]:
    """`<layer>.<namespace>.<table>` as its layer and the table the catalog names."""
    layer, _, table = identifier.partition(".")
    if layer not in LAYERS or table.count(".") < 1:
        raise ValueError(
            f"expected <layer>.<namespace>.<table> with a layer of {', '.join(LAYERS)}, "
            f"got {identifier!r}"
        )
    return layer, table


__all__ = [
    "LAYERS",
    "Storages",
    "split_identifier",
]
