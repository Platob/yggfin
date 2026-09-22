"""Catalogs and namespaces: the CRUD around the tables."""

from __future__ import annotations

import dataclasses
import os
import re
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from yggdryl import Arn, Uri, Url

from rekep.convert import Convertible
from rekep.fields import field_of
from rekep.require import require

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog, PropertiesUpdateSummary
    from pyiceberg.table import Table

    from rekep.iceberg.dataset import IcebergDataset

#: PyIceberg's native PyArrow streams, with yggfin's output ownership boundary.
PYARROW_FILE_IO = "rekep.iceberg.file_io.IcebergFileIO"

#: AWS S3 Tables. A table bucket is served by an Iceberg REST catalog AWS
#: hosts, so this type is the one yggfin resolves itself: it loads pyiceberg's
#: REST catalog against the right regional endpoint, signed for the right
#: service. Which front door is not a second setting -- the warehouse states
#: it, because the two endpoints take the bucket under different names.
S3_TABLES = "s3tables"

#: The S3 Tables endpoint's own name for a bucket: the table bucket ARN, which
#: states the region as well. `https://s3tables.<region>.amazonaws.com/iceberg`
#: serves it, signed for `s3tables`, and Lake Formation is not in that path.
#: `yggdryl.Arn` reads the five AWS fields; what is checked here is that they
#: name a bucket of this service and nothing under it.
_BUCKET_RESOURCE = "bucket"

#: The Glue endpoint's name for the same bucket, once the table bucket is
#: integrated with the AWS analytics services and mounted under the
#: `s3tablescatalog` federated catalog: `<account>:s3tablescatalog/<bucket>`,
#: optionally deeper. `https://glue.<region>.amazonaws.com/iceberg` serves it,
#: signed for `glue`, and Lake Formation governs and vends for it. The name
#: carries no region, so that one comes from the AWS configuration.
_GLUE_TABLE_BUCKET = re.compile(
    r"^(?P<account>\d{12}):s3tablescatalog/(?P<bucket>[a-z0-9][a-z0-9-]{1,61}[a-z0-9])"
    r"(?P<under>(?:/[a-z0-9_-]+)*)$"
)

#: Where a region's AWS endpoints live. Only China is spelled: a `us-gov-` or
#: `us-iso-` region is reached by naming `uri` outright.
_CHINA_DOMAIN = "amazonaws.com.cn"
_AWS_DOMAIN = "amazonaws.com"

#: Properties an operator may have stated the region under, in the order they
#: are read for the Glue door. The S3 Tables door reads none of them: its ARN
#: says which region the bucket is in.
_REGION_PROPERTIES = ("rest.signing-region", "glue.region", "s3.region")

#: Where a region is read from the environment. `AWS_REGION` is here because
#: botocore is not: its session resolves `AWS_DEFAULT_REGION`, a profile and
#: the instance metadata, and `AWS_REGION` -- what a container, a Lambda and
#: this project's own deployment examples set -- is not among them.
_REGION_VARIABLES = ("AWS_REGION", "AWS_DEFAULT_REGION")

_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_WINDOWS_PATH = re.compile(r"^(?:[A-Za-z]:[\\/]|\\\\)")


class TableBucket(NamedTuple):
    """One S3 Tables bucket, as the endpoint that serves it names it."""

    #: The warehouse, exactly as that endpoint takes it.
    warehouse: str

    #: The SigV4 signing name, which is also the endpoint's subdomain:
    #: `s3tables` for the S3 Tables endpoint, `glue` for the Glue one.
    service: str

    #: The region the warehouse itself states, or None where it states none.
    region: str | None


def table_bucket_of(properties: Mapping[str, Any]) -> str | None:
    """The S3 Tables table bucket `properties` names, or None for any other."""
    bucket = _table_bucket(properties)
    return None if bucket is None else bucket.warehouse


def _table_bucket(properties: Mapping[str, Any]) -> TableBucket | None:
    """The table bucket these properties name, and the door it names it at.

    Raises where the type says `s3tables` and the warehouse is neither name a
    bucket has: the warehouse is the configuration, so an unrecognized one is
    a document to fix rather than an endpoint to guess.
    """
    declared = properties.get("type")
    if not isinstance(declared, str) or declared.strip().casefold() != S3_TABLES:
        return None
    warehouse = str(properties.get("warehouse") or "").strip()
    if (arn := _table_bucket_arn(warehouse)) is not None:
        return TableBucket(warehouse, S3_TABLES, arn.region)
    if parsed := _GLUE_TABLE_BUCKET.match(warehouse):
        return TableBucket(parsed.group(0), "glue", None)
    raise ValueError(
        f"an {S3_TABLES} catalog is its table bucket, named as the endpoint "
        "serving it names it: arn:aws:s3tables:<region>:<account>:bucket/<name> "
        "for the S3 Tables endpoint, or <account>:s3tablescatalog/<name> for "
        f"the Glue one; not {warehouse!r}"
    )


def _table_bucket_arn(warehouse: str) -> Arn | None:
    """`warehouse` as the ARN of one table bucket, or None for anything else.

    An ARN of another service, one naming no region, or one reaching under
    the bucket -- a table, a namespace -- is not the endpoint's name for a
    bucket, so it is none: the caller says what a warehouse may be.
    """
    if not warehouse.startswith("arn:"):
        return None
    try:
        arn = Arn.from_str(warehouse)
    except ValueError:
        return None
    if (
        arn.service != S3_TABLES
        or arn.resource_type != _BUCKET_RESOURCE
        or arn.region is None
        or "/" in arn.resource_id
    ):
        return None
    return arn


def _s3_tables_properties(properties: Mapping[str, str]) -> dict[str, str]:
    """PyIceberg's REST configuration for the table bucket these name.

    Only what the warehouse decides is filled, and each of it with
    `setdefault`: a private or FIPS endpoint, another signing region, and
    explicit credentials stay the operator's to state, under the standard
    property names.
    """
    bucket = _table_bucket(properties)
    if bucket is None:
        return dict(properties)
    # pyiceberg signs the REST calls with botocore and resolves credentials
    # through a boto3 session; neither comes with the Iceberg extra.
    require("boto3", S3_TABLES)
    region = bucket.region or _stated_region(properties)
    domain = _CHINA_DOMAIN if region.startswith("cn-") else _AWS_DOMAIN
    resolved = dict(properties)
    resolved["type"] = "rest"
    # The warehouse as it parsed: pyiceberg sends it to the endpoint, and a
    # document may have spelled it with space around it.
    resolved["warehouse"] = bucket.warehouse
    resolved.setdefault("uri", f"https://{bucket.service}.{region}.{domain}/iceberg")
    resolved.setdefault("rest.sigv4-enabled", "true")
    resolved.setdefault("rest.signing-name", bucket.service)
    resolved.setdefault("rest.signing-region", region)
    # The bucket is in that region, and so are the files the endpoint vends
    # credentials for.
    resolved.setdefault("s3.region", region)
    return resolved


def _stated_region(properties: Mapping[str, str]) -> str:
    """The region for a warehouse that names none: the AWS configuration's.

    The Glue door takes the bucket as `<account>:s3tablescatalog/<bucket>`,
    which says nothing about where either is. A region is stated once, under a
    property or in the environment every AWS client here already reads, and
    never guessed: the wrong one signs for a catalog that is not this one.
    """
    for name in _REGION_PROPERTIES:
        if stated := str(properties.get(name) or "").strip():
            return stated
    if environment := _environment_region():
        return environment
    raise ValueError(
        f"an {S3_TABLES} catalog at the Glue endpoint states its region: set "
        "rest.signing-region on the catalog, or AWS_REGION in the worker's "
        "environment"
    )


def _environment_region() -> str | None:
    """The region the AWS environment states, or None."""
    for name in _REGION_VARIABLES:
        if stated := os.environ.get(name, "").strip():
            return stated
    import boto3

    # A profile, a config file, or the instance the worker runs on.
    return boto3.Session().region_name


def _file_location(location: str) -> str:
    """Make a local path absolute while leaving an explicit URI on its store."""
    if location.casefold().startswith("file:"):
        uri = Uri(location)
        try:
            return str(uri.into_url())
        except ValueError:
            # Arrow accepts `file:/x`; Yggdryl Url deliberately requires the
            # hierarchical `file:///x`. Resolve the URI as a path first.
            return str(Url.from_path(Path(uri.into_path()).resolve()))
    if os.name == "nt" and _WINDOWS_PATH.match(location):
        return str(Url.from_path(Path(location).resolve()))
    if _SCHEME.match(location):
        return location
    return str(Url.from_path(Path(location).resolve()))


@dataclasses.dataclass(eq=False)
class IcebergCatalog(Convertible):
    """One pyiceberg catalog, with the verbs a stack needs."""

    name: str = "default"
    properties: dict[str, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        """Freeze the warehouse location before this handle is shared."""
        import threading

        if not isinstance(self.name, str):
            raise TypeError("an Iceberg catalog name must be a string")
        if not self.name:
            raise ValueError("an Iceberg catalog name must be non-empty")
        self.properties = dict(self.properties)
        warehouse = self.properties.get("warehouse")
        # A table bucket is named, not located: `123456789012:s3tablescatalog/x`
        # is no more a relative path than the ARN is, and resolving it against
        # the working directory produced a `file://` URL for the endpoint.
        if warehouse and table_bucket_of(self.properties) is None:
            self.properties["warehouse"] = _file_location(warehouse)
        self.__dict__["_location_guard"] = threading.RLock()

    # -- the catalog --------------------------------------------------------

    @property
    def catalog(self) -> Catalog:
        """The pyiceberg catalog, loaded once.

        Loading reads configuration and may open a connection, and every table
        here lives in the same one. `py-io-impl` defaults to Arrow's FileIO;
        a named implementation is wrapped so failed commits still own every
        output they created. A `s3tables` type resolves here rather than in
        `__post_init__`, so a bucket named by a later `--property` resolves
        too.
        """
        loaded = self.__dict__.get("catalog")
        if loaded is not None:
            return loaded
        with self.__dict__["_location_guard"]:
            loaded = self.__dict__.get("catalog")
            if loaded is not None:
                return loaded
            require("pyiceberg", "iceberg")
            from pyiceberg.catalog import load_catalog

            properties = self._tracked_file_io_properties(_s3_tables_properties(self.properties))
            loaded = load_catalog(self.name, **properties)
            self.__dict__["catalog"] = loaded
            return loaded

    @staticmethod
    def _tracked_file_io_properties(properties: Mapping[str, str]) -> dict[str, str]:
        """Catalog properties with custom FileIO routed through ownership tracking."""
        from rekep.iceberg.file_io import DELEGATE_FILE_IO, TRACKED_FILE_IO

        configured = dict(properties)
        implementation = configured.get("py-io-impl")
        if implementation and implementation not in {PYARROW_FILE_IO, TRACKED_FILE_IO}:
            configured[DELEGATE_FILE_IO] = implementation
            configured["py-io-impl"] = TRACKED_FILE_IO
        else:
            configured.setdefault("py-io-impl", PYARROW_FILE_IO)
        return configured

    @property
    def table_bucket(self) -> str | None:
        """The S3 Tables table bucket this catalog is, or None for any other.

        A table bucket's files are the service's: S3 Tables compacts them and
        expires their snapshots on a schedule of its own, and the bucket
        behind a table is not one this account lists. Maintenance reads this
        and leaves the sweep to whoever owns the files.
        """
        return table_bucket_of(self.properties)

    def close(self) -> None:
        """Release a loaded catalog without opening an unused one."""
        catalog = self.__dict__.pop("catalog", None)
        if catalog is not None:
            catalog.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    # -- namespaces ---------------------------------------------------------

    def namespaces(self, under: str | None = None, *, recursive: bool = False) -> list[str]:
        """Namespace names, dotted, under `under` or at the top.

        `list_namespaces` is one level deep -- `trading` does not bring back
        `trading.eu` -- so `recursive=True` walks down. It costs one call per
        namespace found, which is free on SQLite and a round trip each on REST
        or Glue; the default stays the single call.

        A table bucket has no second level to walk: S3 Tables namespaces are
        one deep, whichever endpoint serves them, so the walk there would be
        one round trip per namespace to be told so.
        """
        found = self.catalog.list_namespaces(*((under,) if under else ()))
        names = [".".join(levels) for levels in found]
        if not recursive or self.table_bucket:
            return names
        below = [name for parent in names for name in self.namespaces(parent, recursive=True)]
        return list(dict.fromkeys(names + below))

    def namespace(self, name: str) -> IcebergNamespace:
        """A handle on one namespace, whether or not it exists yet."""
        return IcebergNamespace(catalog=self, name=name)

    def create_namespace(
        self, name: str, properties: dict[str, str] | None = None, *, exists_ok: bool = True
    ) -> IcebergNamespace:
        """Create a namespace, or leave the one that is there alone."""
        if exists_ok:
            self.catalog.create_namespace_if_not_exists(name, properties or {})
        else:
            self.catalog.create_namespace(name, properties or {})
        return self.namespace(name)

    def drop_namespace(self, name: str, *, missing_ok: bool = True) -> None:
        """Drop a namespace; it has to be empty, as Iceberg requires."""
        if missing_ok and not self.namespace_exists(name):
            return
        self.catalog.drop_namespace(name)

    def namespace_exists(self, name: str) -> bool:
        return bool(self.catalog.namespace_exists(name))

    def namespace_properties(self, name: str) -> dict[str, str]:
        return dict(self.catalog.load_namespace_properties(name))

    def update_namespace_properties(
        self, name: str, updates: dict[str, str] | None = None, removals: set[str] | None = None
    ) -> PropertiesUpdateSummary:
        return self.catalog.update_namespace_properties(name, removals or set(), updates or {})

    # -- tables -------------------------------------------------------------

    def tables(self, namespace: str | None = None) -> list[str]:
        """Table identifiers, dotted: one namespace's, or every namespace's.

        *Every* namespace means nested ones too. A `list_namespaces` with no
        argument returns only the top level, so `trading.eu.paris` was silently
        missing -- and a sweep written as `for dataset in catalog.datasets()`
        never touched those tables, without reporting a skip.
        """
        spaces = [namespace] if namespace else self.namespaces(recursive=True)
        return [
            ".".join(identifier)
            for space in spaces
            for identifier in self.catalog.list_tables(space)
        ]

    def table_exists(self, name: str) -> bool:
        return bool(self.catalog.table_exists(name))

    def load_table(self, name: str) -> Table:
        """The pyiceberg table, for what this package does not wrap."""
        return self.catalog.load_table(name)

    def drop_table(self, name: str, *, purge: bool = False, missing_ok: bool = True) -> None:
        """Drop a table, optionally deleting its files with it.

        A table in a bucket has one drop and it takes the data: S3 Tables owns
        those files, so it answers a drop that asks to keep them with a 400.
        `purge` is a choice everywhere else.
        """
        if missing_ok and not self.table_exists(name):
            return
        if purge or self.table_bucket:
            self.catalog.purge_table(name)
        else:
            self.catalog.drop_table(name)

    def rename_table(self, name: str, to: str) -> Table:
        return self.catalog.rename_table(name, to)

    def dataset(self, name: str, *, namespace: str | None = None, **kwargs: Any) -> IcebergDataset:
        """A dataset on this catalog: the way to read and write a table here.

        A dotted `name` supplies its namespace. Passing `namespace` keeps the
        table name and schema name visibly separate. Without a field, the
        existing table is loaded once and its declaration is read back.

        Handed *this* catalog rather than left to build its own, the way
        `create_with_field` hands over the table it just made: loading a
        pyiceberg catalog builds a SQLAlchemy engine or asks a REST server for
        its config, and `datasets()` was paying that per table.
        """
        from rekep.iceberg.dataset import IcebergDataset

        namespace, name = _coordinates(name, namespace)
        identifier = f"{namespace}.{name}"
        field = kwargs.pop("field", None)
        table = None
        if field is None:
            table = self.load_table(identifier)
            from rekep.iceberg.fields import iceberg_struct_field

            field = iceberg_struct_field(
                table.schema(),
                name,
                spec=table.spec(),
                sort_order=table.sort_order(),
            )
        else:
            field = field_of(field, name)
        built = IcebergDataset(
            name=name,
            namespace=namespace,
            field=field,
            catalog_name=self.name,
            catalog_properties=self.properties,
            **kwargs,
        )
        built.__dict__["store"] = self
        built.__dict__["_owns_store"] = False
        if table is not None:
            built.__dict__["iceberg_table"] = table
        return built

    def datasets(self, namespace: str | None = None) -> Iterator[IcebergDataset]:
        """One dataset per table, for a sweep over a whole namespace."""
        for identifier in self.tables(namespace):
            yield self.dataset(identifier)


@dataclasses.dataclass(eq=False)
class IcebergNamespace(Convertible):
    """One namespace in a catalog: its properties, its tables, its datasets."""

    catalog: IcebergCatalog
    name: str

    @property
    def exists(self) -> bool:
        return self.catalog.namespace_exists(self.name)

    def create(self, properties: dict[str, str] | None = None) -> IcebergNamespace:
        """Create it if it is not there; hand it back either way."""
        self.catalog.create_namespace(self.name, properties)
        return self

    def drop(self, *, missing_ok: bool = True) -> None:
        self.catalog.drop_namespace(self.name, missing_ok=missing_ok)

    @property
    def properties(self) -> dict[str, str]:
        return self.catalog.namespace_properties(self.name)

    def update_properties(
        self, updates: dict[str, str] | None = None, removals: set[str] | None = None
    ) -> PropertiesUpdateSummary:
        return self.catalog.update_namespace_properties(self.name, updates, removals)

    def tables(self) -> list[str]:
        return self.catalog.tables(self.name)

    def dataset(self, name: str, **kwargs: Any) -> IcebergDataset:
        """A dataset for a table in this namespace, named without the prefix."""
        return self.catalog.dataset(name, namespace=self.name, **kwargs)


def _coordinates(name: str, namespace: str | None) -> tuple[str, str]:
    """A table's explicit namespace and unqualified name."""
    if namespace is None:
        namespace, separator, name = name.rpartition(".")
        if not separator:
            raise ValueError("an Iceberg table name needs an explicit namespace")
    elif "." in name:
        raise ValueError("pass either a dotted name or namespace=, not both")
    if not name or not namespace or any(not part for part in namespace.split(".")):
        raise ValueError("an Iceberg table needs a non-empty name and namespace")
    return namespace, name
