"""Catalog and namespace CRUD against a real, fully local catalog."""

import os
from pathlib import Path
from typing import Annotated

import pyarrow.fs
import pytest

from rekep import Convertible, scalar
from rekep.fields import primary_key
from rekep.iceberg import IcebergCatalog, IcebergDataset
from rekep.iceberg.catalog import PYARROW_FILE_IO
from rekep.iceberg.file_io import IcebergFileIO


@scalar
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, primary_key()]
    """Instrument."""

    size: int
    """Quantity."""


class CustomArrowFileIO(IcebergFileIO):
    """A distinct configured FileIO."""


@pytest.fixture
def catalog(tmp_path: Path) -> IcebergCatalog:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    return IcebergCatalog(
        name="test",
        properties={
            "type": "sql",
            "uri": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
            "warehouse": warehouse.as_uri(),
        },
    )


# -- the catalog ------------------------------------------------------------


def test_arrow_is_the_default_file_io(catalog: IcebergCatalog) -> None:
    """Everything else here reads and writes through pyarrow.fs; so does Iceberg."""
    assert catalog.catalog.properties["py-io-impl"] == PYARROW_FILE_IO


def test_standard_s3_properties_reach_pyiceberg_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pyiceberg.catalog

    seen = {}

    def loaded(name: str, **properties: str) -> object:
        seen.update(properties)
        return object()

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", loaded)
    properties = {
        "type": "in-memory",
        "warehouse": "s3://bucket/warehouse",
        "s3.endpoint": "http://minio:9000",
        "s3.region": "eu-west-1",
        "s3.access-key-id": "key",
        "s3.secret-access-key": "secret",
    }
    _ = IcebergCatalog(properties=properties).catalog
    assert {name: seen[name] for name in properties} == properties


#: One table bucket, in the partition, region and account its ARN states.
TABLE_BUCKET = "arn:aws:s3tables:eu-west-1:123456789012:bucket/market-tables"


def _loaded(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """The properties pyiceberg is handed when a catalog here is loaded."""
    import pyiceberg.catalog

    seen: dict[str, str] = {}

    def load(name: str, **properties: str) -> object:
        seen.update(properties)
        return object()

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", load)
    return seen


def test_a_table_bucket_arn_resolves_the_s3_tables_rest_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`s3tables` is the REST catalog AWS hosts; the ARN states where it is."""
    seen = _loaded(monkeypatch)

    catalog = IcebergCatalog(
        name="rekep", properties={"type": "s3tables", "warehouse": TABLE_BUCKET}
    )
    _ = catalog.catalog

    assert seen["type"] == "rest"
    assert seen["warehouse"] == TABLE_BUCKET
    assert seen["uri"] == "https://s3tables.eu-west-1.amazonaws.com/iceberg"
    assert seen["rest.sigv4-enabled"] == "true"
    assert seen["rest.signing-name"] == "s3tables"
    assert seen["rest.signing-region"] == "eu-west-1"
    assert seen["s3.region"] == "eu-west-1"
    # The type the document names is what the document still says it is.
    assert catalog.properties["type"] == "s3tables"
    assert catalog.table_bucket == TABLE_BUCKET


def test_a_table_bucket_keeps_every_setting_the_operator_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A private endpoint, another signing region, explicit keys: all stated."""
    seen = _loaded(monkeypatch)

    _ = IcebergCatalog(
        properties={
            "type": "s3tables",
            "warehouse": TABLE_BUCKET,
            "uri": "https://vpce-0abc.s3tables.eu-west-1.vpce.amazonaws.com/iceberg",
            "rest.signing-region": "eu-west-2",
            "s3.region": "eu-west-2",
        }
    ).catalog

    assert seen["uri"] == "https://vpce-0abc.s3tables.eu-west-1.vpce.amazonaws.com/iceberg"
    assert seen["rest.signing-region"] == "eu-west-2"
    assert seen["s3.region"] == "eu-west-2"
    assert seen["rest.signing-name"] == "s3tables"


def test_a_table_bucket_in_another_partition_keeps_its_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = _loaded(monkeypatch)

    _ = IcebergCatalog(
        properties={
            "type": "s3tables",
            "warehouse": "arn:aws-cn:s3tables:cn-north-1:123456789012:bucket/market",
        }
    ).catalog

    assert seen["uri"] == "https://s3tables.cn-north-1.amazonaws.com.cn/iceberg"


#: The same bucket, as the `s3tables:` locator its ARN redirects to: the two
#: fields a location does not carry are stated in the query, spelled the way
#: every store URL here spells them.
LOCATED_BUCKET = "s3tables://market-tables?region=eu-west-1&account=123456789012"


def test_a_table_bucket_locator_is_the_arn_it_spells(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ARN redirects to its `s3tables:` URL through `Arn.locator()`, and
    the URL resolves back to the ARN the endpoint takes: one bucket, two
    spellings, one configuration handed to pyiceberg."""
    seen = _loaded(monkeypatch)

    catalog = IcebergCatalog(properties={"type": "s3tables", "warehouse": LOCATED_BUCKET})
    _ = catalog.catalog

    assert seen["type"] == "rest"
    assert seen["warehouse"] == TABLE_BUCKET
    assert seen["uri"] == "https://s3tables.eu-west-1.amazonaws.com/iceberg"
    assert seen["rest.signing-name"] == "s3tables"
    assert seen["rest.signing-region"] == "eu-west-1"
    assert seen["s3.region"] == "eu-west-1"
    # The document still says what it said; the endpoint is handed the ARN.
    assert catalog.properties["warehouse"] == LOCATED_BUCKET
    assert catalog.table_bucket == TABLE_BUCKET


@pytest.mark.parametrize(
    ("warehouse", "endpoint"),
    [
        # A private endpoint, spelled as every store URL here spells one.
        (
            "s3tables://market-tables?region=eu-west-1&account=123456789012"
            "&endpoint_override=vpce-0abc.s3tables.eu-west-1.vpce.amazonaws.com",
            "https://vpce-0abc.s3tables.eu-west-1.vpce.amazonaws.com/iceberg",
        ),
        # The endpoint as the location's own host, with a port and a scheme:
        # an emulator of the service on a developer's machine.
        (
            "s3tables://localhost:9001/market-tables?scheme=http&region=eu-west-1"
            "&account=123456789012",
            "http://localhost:9001/iceberg",
        ),
        # A FIPS endpoint, by its host.
        (
            "s3tables://s3tables-fips.us-east-1.amazonaws.com/market-tables"
            "?region=us-east-1&account=123456789012",
            "https://s3tables-fips.us-east-1.amazonaws.com/iceberg",
        ),
    ],
    ids=["endpoint_override", "host and port", "fips host"],
)
def test_a_table_bucket_locator_states_where_its_endpoint_is(
    monkeypatch: pytest.MonkeyPatch, warehouse: str, endpoint: str
) -> None:
    """What the locator can say that the ARN cannot: where the catalog is."""
    seen = _loaded(monkeypatch)

    catalog = IcebergCatalog(properties={"type": "s3tables", "warehouse": warehouse})
    _ = catalog.catalog

    assert seen["uri"] == endpoint
    assert seen["rest.signing-name"] == "s3tables"
    assert seen["warehouse"].startswith("arn:aws:s3tables:")
    assert seen["warehouse"].endswith(":123456789012:bucket/market-tables")
    assert seen["rest.signing-region"] == seen["s3.region"] == catalog.table_bucket.split(":")[3]


def test_a_table_bucket_locator_in_another_partition_spells_its_arn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = _loaded(monkeypatch)

    _ = IcebergCatalog(
        properties={
            "type": "s3tables",
            "warehouse": "s3tables://market?region=cn-north-1&account=123456789012&partition=aws-cn",
        }
    ).catalog

    assert seen["warehouse"] == "arn:aws-cn:s3tables:cn-north-1:123456789012:bucket/market"
    assert seen["uri"] == "https://s3tables.cn-north-1.amazonaws.com.cn/iceberg"


def test_a_table_bucket_locator_takes_the_region_the_configuration_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A locator that names no region is read like the Glue name: once, off
    the catalog or the environment, never guessed."""
    seen = _loaded(monkeypatch)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.setenv("AWS_REGION", "us-east-2")

    catalog = IcebergCatalog(
        properties={"type": "s3tables", "warehouse": "s3tables://market?account=123456789012"}
    )
    _ = catalog.catalog

    assert catalog.table_bucket == "arn:aws:s3tables:us-east-2:123456789012:bucket/market"
    assert seen["uri"] == "https://s3tables.us-east-2.amazonaws.com/iceberg"


@pytest.mark.parametrize(
    ("warehouse", "refused"),
    [
        # The endpoint takes the bucket under its ARN, and an ARN names the
        # account; a location does not, so the locator states it.
        ("s3tables://market?region=eu-west-1", "account"),
        # A table's locator names a table, which is not a catalog.
        ("s3tables://market/t-a1?region=eu-west-1&account=123456789012", "under one"),
        ("arn:aws:s3tables:eu-west-1:123456789012:bucket/market/table/t-a1", "under one"),
        # An ARN naming every region names no endpoint.
        ("arn:aws:s3tables::123456789012:bucket/market", "region"),
        # Another service's ARN, and a location of another store.
        ("arn:aws:s3:::market", "s3tablescatalog"),
        ("s3://market/rekep", "s3tablescatalog"),
    ],
    ids=["no account", "a table", "a table arn", "no region", "s3 arn", "s3 url"],
)
def test_what_is_not_a_table_bucket_is_refused_by_name(warehouse: str, refused: str) -> None:
    with pytest.raises(ValueError, match=refused):
        IcebergCatalog(properties={"type": "s3tables", "warehouse": warehouse})


def test_a_table_bucket_arn_is_left_exactly_as_written() -> None:
    """`arn:` is a scheme, so nothing resolves it against the working directory."""
    catalog = IcebergCatalog(properties={"type": "s3tables", "warehouse": TABLE_BUCKET})

    assert catalog.properties["warehouse"] == TABLE_BUCKET


def test_s3_tables_without_a_table_bucket_says_so() -> None:
    """The warehouse is the configuration, so a prefix is a document to fix."""
    with pytest.raises(ValueError, match="s3tablescatalog"):
        IcebergCatalog(properties={"type": "s3tables", "warehouse": "s3://market/rekep"})


#: The same bucket, as the Glue endpoint names it once the table bucket is
#: integrated and mounted under the `s3tablescatalog` federated catalog.
GLUE_TABLE_BUCKET = "123456789012:s3tablescatalog/market-tables"


def test_a_federated_table_bucket_resolves_the_glue_rest_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other door onto the same bucket: Glue's, signed for Glue."""
    seen = _loaded(monkeypatch)

    catalog = IcebergCatalog(
        name="rekep",
        properties={
            "type": "s3tables",
            "warehouse": GLUE_TABLE_BUCKET,
            "rest.signing-region": "eu-west-1",
        },
    )
    _ = catalog.catalog

    assert seen["type"] == "rest"
    assert seen["warehouse"] == GLUE_TABLE_BUCKET
    assert seen["uri"] == "https://glue.eu-west-1.amazonaws.com/iceberg"
    assert seen["rest.sigv4-enabled"] == "true"
    assert seen["rest.signing-name"] == "glue"
    assert seen["s3.region"] == "eu-west-1"
    assert catalog.table_bucket == GLUE_TABLE_BUCKET


def test_a_federated_table_bucket_is_not_resolved_against_the_directory() -> None:
    """It begins with digits, so the local-path rule must not claim it."""
    catalog = IcebergCatalog(properties={"type": "s3tables", "warehouse": GLUE_TABLE_BUCKET})

    assert catalog.properties["warehouse"] == GLUE_TABLE_BUCKET


def test_a_federated_table_bucket_takes_the_region_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`AWS_REGION` is read here because botocore's session does not read it."""
    seen = _loaded(monkeypatch)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.setenv("AWS_REGION", "us-east-2")

    _ = IcebergCatalog(properties={"type": "s3tables", "warehouse": GLUE_TABLE_BUCKET}).catalog

    assert seen["uri"] == "https://glue.us-east-2.amazonaws.com/iceberg"
    assert seen["rest.signing-region"] == "us-east-2"


def test_a_federated_table_bucket_without_a_region_says_where_to_state_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing here guesses one: the wrong region signs for another catalog."""
    from rekep.iceberg import catalog as module

    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.setattr(module, "_environment_region", lambda: None)
    store = IcebergCatalog(properties={"type": "s3tables", "warehouse": GLUE_TABLE_BUCKET})

    with pytest.raises(ValueError, match="rest.signing-region"):
        _ = store.catalog


def test_nothing_in_the_environment_still_reaches_the_public_regional_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No variable set is the ordinary run: AWS's own endpoint, and Arrow's
    own S3 endpoint for the files, with nothing stated for either."""
    seen = _loaded(monkeypatch)

    _ = IcebergCatalog(properties={"type": "s3tables", "warehouse": TABLE_BUCKET}).catalog

    assert seen["uri"] == "https://s3tables.eu-west-1.amazonaws.com/iceberg"
    assert "s3.endpoint" not in seen


@pytest.mark.parametrize(
    ("warehouse", "variable", "service"),
    [
        (TABLE_BUCKET, "AWS_ENDPOINT_URL_S3TABLES", "s3tables"),
        (LOCATED_BUCKET, "AWS_ENDPOINT_URL_S3TABLES", "s3tables"),
        (GLUE_TABLE_BUCKET, "AWS_ENDPOINT_URL_GLUE", "glue"),
    ],
    ids=["arn", "locator", "glue"],
)
def test_the_environment_states_the_endpoint_of_the_door_the_warehouse_names(
    monkeypatch: pytest.MonkeyPatch, warehouse: str, variable: str, service: str
) -> None:
    """`AWS_ENDPOINT_URL_<SERVICE>` is where the CLI reads a service's
    endpoint, and the catalog answers under `/iceberg` there. The files are
    in S3 behind either door."""
    seen = _loaded(monkeypatch)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv(variable, "http://localhost:4566")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")

    catalog = IcebergCatalog(properties={"type": "s3tables", "warehouse": warehouse})
    _ = catalog.catalog

    assert seen["uri"] == "http://localhost:4566/iceberg"
    assert seen["rest.signing-name"] == service
    assert seen["s3.endpoint"] == "http://localhost:9000"
    # The endpoint moves; the name it takes the bucket under does not.
    assert seen["warehouse"] == catalog.table_bucket


@pytest.mark.parametrize(
    ("warehouse", "variable", "regional"),
    [
        (TABLE_BUCKET, "AWS_ENDPOINT_URL_GLUE", "https://s3tables.eu-west-1.amazonaws.com/iceberg"),
        (
            GLUE_TABLE_BUCKET,
            "AWS_ENDPOINT_URL_S3TABLES",
            "https://glue.eu-west-1.amazonaws.com/iceberg",
        ),
    ],
    ids=["arn ignores glue", "glue ignores s3tables"],
)
def test_each_door_reads_only_its_own_variable(
    monkeypatch: pytest.MonkeyPatch, warehouse: str, variable: str, regional: str
) -> None:
    seen = _loaded(monkeypatch)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv(variable, "http://localhost:4566")

    _ = IcebergCatalog(properties={"type": "s3tables", "warehouse": warehouse}).catalog

    assert seen["uri"] == regional


@pytest.mark.parametrize(
    ("stated", "endpoint"),
    [
        ("http://localhost:4566", "http://localhost:4566/iceberg"),
        ("http://localhost:4566/", "http://localhost:4566/iceberg"),
        ("http://localhost:4566/iceberg", "http://localhost:4566/iceberg"),
        ("http://localhost:4566/iceberg/", "http://localhost:4566/iceberg"),
        ("  http://localhost:4566  ", "http://localhost:4566/iceberg"),
        ("http://proxy:8080/aws", "http://proxy:8080/aws/iceberg"),
        # A host named `iceberg` -- a compose service -- has no path yet.
        ("http://iceberg", "http://iceberg/iceberg"),
        ("http://iceberg/", "http://iceberg/iceberg"),
    ],
    ids=[
        "host",
        "trailing slash",
        "with path",
        "path and slash",
        "padded",
        "path prefix",
        "host named iceberg",
        "host named iceberg and slash",
    ],
)
def test_an_environment_endpoint_is_given_its_path_once(
    monkeypatch: pytest.MonkeyPatch, stated: str, endpoint: str
) -> None:
    seen = _loaded(monkeypatch)
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3TABLES", stated)

    _ = IcebergCatalog(properties={"type": "s3tables", "warehouse": TABLE_BUCKET}).catalog

    assert seen["uri"] == endpoint


@pytest.mark.parametrize(
    ("warehouse", "regional"),
    [
        (TABLE_BUCKET, "https://s3tables.eu-west-1.amazonaws.com/iceberg"),
        (GLUE_TABLE_BUCKET, "https://glue.eu-west-1.amazonaws.com/iceberg"),
    ],
    ids=["s3tables", "glue"],
)
def test_the_generic_endpoint_names_where_the_files_are_and_not_the_catalog(
    monkeypatch: pytest.MonkeyPatch, warehouse: str, regional: str
) -> None:
    """`AWS_ENDPOINT_URL` names one endpoint for every service. The files are
    read through S3 alone, so it is theirs; the catalog could be either of two
    doors, and one value cannot be the right host for both."""
    seen = _loaded(monkeypatch)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566/")

    _ = IcebergCatalog(properties={"type": "s3tables", "warehouse": warehouse}).catalog

    assert seen["uri"] == regional
    assert seen["s3.endpoint"] == "http://localhost:4566"


def test_the_s3_variable_names_where_the_files_are(monkeypatch: pytest.MonkeyPatch) -> None:
    """`AWS_ENDPOINT_URL_S3` is read before the generic variable, as the CLI reads it."""
    seen = _loaded(monkeypatch)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000/")

    _ = IcebergCatalog(properties={"type": "s3tables", "warehouse": TABLE_BUCKET}).catalog

    assert seen["s3.endpoint"] == "http://localhost:9000"


def test_the_document_then_the_locator_then_the_environment_state_the_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The environment is a default for every table bucket the worker runs,
    and a document names one catalog: what the document states wins, and
    what it leaves unstated is still the environment's."""
    seen = _loaded(monkeypatch)
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3TABLES", "http://localhost:4566")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://localhost:4566")
    located = LOCATED_BUCKET + "&endpoint_override=vpce-0abc.s3tables.eu-west-1.vpce.amazonaws.com"

    _ = IcebergCatalog(properties={"type": "s3tables", "warehouse": located}).catalog
    assert seen["uri"] == "https://vpce-0abc.s3tables.eu-west-1.vpce.amazonaws.com/iceberg"
    assert seen["s3.endpoint"] == "http://localhost:4566"

    _ = IcebergCatalog(
        properties={
            "type": "s3tables",
            "warehouse": located,
            "uri": "https://s3tables.eu-west-1.example.net/iceberg",
        }
    ).catalog
    assert seen["uri"] == "https://s3tables.eu-west-1.example.net/iceberg"
    assert seen["s3.endpoint"] == "http://localhost:4566"

    _ = IcebergCatalog(
        properties={
            "type": "s3tables",
            "warehouse": located,
            "uri": "https://s3tables.eu-west-1.example.net/iceberg",
            "s3.endpoint": "https://s3.eu-west-1.example.net",
        }
    ).catalog
    assert seen["uri"] == "https://s3tables.eu-west-1.example.net/iceberg"
    assert seen["s3.endpoint"] == "https://s3.eu-west-1.example.net"


@pytest.mark.parametrize(
    "variable",
    ["AWS_ENDPOINT_URL_S3TABLES", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"],
)
def test_an_empty_variable_states_nothing(monkeypatch: pytest.MonkeyPatch, variable: str) -> None:
    seen = _loaded(monkeypatch)
    monkeypatch.setenv(variable, " ")

    _ = IcebergCatalog(properties={"type": "s3tables", "warehouse": TABLE_BUCKET}).catalog

    assert seen["uri"] == "https://s3tables.eu-west-1.amazonaws.com/iceberg"
    assert "s3.endpoint" not in seen


def test_the_environment_can_turn_its_endpoints_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """`AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` switches off every endpoint
    variable the CLI reads, so it switches them off here too."""
    seen = _loaded(monkeypatch)
    monkeypatch.setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "True")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3TABLES", "http://localhost:4566")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://localhost:4566")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")

    _ = IcebergCatalog(properties={"type": "s3tables", "warehouse": TABLE_BUCKET}).catalog

    assert seen["uri"] == "https://s3tables.eu-west-1.amazonaws.com/iceberg"
    assert "s3.endpoint" not in seen


@pytest.mark.parametrize("value", ["false", "0", " true"])
def test_only_true_turns_the_environment_off(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    """botocore reads the switch as a boolean only `true` sets, so any other
    value leaves the environment's endpoints in force."""
    seen = _loaded(monkeypatch)
    monkeypatch.setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", value)
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3TABLES", "http://localhost:4566")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")

    _ = IcebergCatalog(properties={"type": "s3tables", "warehouse": TABLE_BUCKET}).catalog

    assert seen["uri"] == "http://localhost:4566/iceberg"
    assert seen["s3.endpoint"] == "http://localhost:9000"


def test_the_environment_leaves_every_other_catalog_as_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only a table bucket is resolved here; any other type is pyiceberg's."""
    seen = _loaded(monkeypatch)
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://localhost:4566")
    monkeypatch.setenv("AWS_ENDPOINT_URL_GLUE", "http://localhost:4566")

    _ = IcebergCatalog(
        properties={"type": "glue", "warehouse": "s3://market/rekep", "glue.region": "eu-west-1"}
    ).catalog

    assert "s3.endpoint" not in seen
    assert "uri" not in seen


def test_a_bucket_is_listed_one_level_deep() -> None:
    """S3 Tables namespaces are one deep; walking asks per namespace for nothing."""

    class Fake:
        def __init__(self) -> None:
            self.parents: list[tuple[str, ...]] = []

        def list_namespaces(self, *under: str) -> list[tuple[str, ...]]:
            self.parents.append(under)
            return [("logs",), ("fix",)]

    bucket = IcebergCatalog(properties={"type": "s3tables", "warehouse": TABLE_BUCKET})
    bucket.__dict__["catalog"] = (listed := Fake())

    assert bucket.namespaces(recursive=True) == ["logs", "fix"]
    assert listed.parents == [()]


def test_dropping_a_table_in_a_bucket_takes_its_data() -> None:
    """S3 Tables refuses a drop that would keep the files, so this never asks."""

    class Fake:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def table_exists(self, _name: str) -> bool:
            return True

        def drop_table(self, _name: str) -> None:
            self.calls.append("drop")

        def purge_table(self, _name: str) -> None:
            self.calls.append("purge")

    bucket = IcebergCatalog(properties={"type": "s3tables", "warehouse": TABLE_BUCKET})
    bucket.__dict__["catalog"] = (in_bucket := Fake())
    elsewhere = IcebergCatalog(properties={"type": "sql"})
    elsewhere.__dict__["catalog"] = (in_sql := Fake())

    bucket.drop_table("logs.messages")
    elsewhere.drop_table("logs.messages")

    assert in_bucket.calls == ["purge"]
    assert in_sql.calls == ["drop"]


def test_only_an_s3_tables_catalog_is_a_table_bucket() -> None:
    catalog = IcebergCatalog(properties={"type": "glue", "warehouse": TABLE_BUCKET})

    assert catalog.table_bucket is None


def test_relative_local_locations_become_absolute_file_uris(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    catalog = IcebergCatalog(properties={"warehouse": "data/warehouse"})
    dataset = catalog.dataset(
        "trading.quotes",
        field=Quote.into_field(),
        location="data/tables/quotes",
        table_properties={
            "write.data.path": "data/files",
            "write.metadata.path": "data/metadata",
        },
    )

    assert catalog.properties["warehouse"] == (tmp_path / "data/warehouse").as_uri()
    assert dataset.location == (tmp_path / "data/tables/quotes").as_uri()
    assert dataset.table_properties == {
        "write.data.path": (tmp_path / "data/files").as_uri(),
        "write.metadata.path": (tmp_path / "data/metadata").as_uri(),
    }


def test_a_nonhierarchical_file_uri_becomes_a_canonical_url(tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse"
    shorthand = f"file:{warehouse.as_posix()}"

    catalog = IcebergCatalog(properties={"warehouse": shorthand})

    assert catalog.properties["warehouse"] == warehouse.as_uri()


def test_a_file_uri_with_unc_backslashes_keeps_its_authority() -> None:
    catalog = IcebergCatalog(properties={"warehouse": r"file:\\server\share\warehouse"})

    assert catalog.properties["warehouse"] == "file://server/share/warehouse"


def test_a_named_file_io_wins(tmp_path: Path) -> None:
    named = IcebergCatalog(name="test", properties={"type": "in-memory", "py-io-impl": "x.Y"})
    assert named.properties["py-io-impl"] == "x.Y"


def test_a_named_file_io_is_wrapped_with_output_ownership(tmp_path: Path) -> None:
    from rekep.iceberg.file_io import TRACKED_FILE_IO, TrackedFileIO

    warehouse = tmp_path / "custom-warehouse"
    warehouse.mkdir()
    catalog = IcebergCatalog(
        name="custom",
        properties={
            "type": "sql",
            "uri": f"sqlite:///{(tmp_path / 'custom.db').as_posix()}",
            "warehouse": warehouse.as_uri(),
            "py-io-impl": f"{__name__}.CustomArrowFileIO",
        },
    )
    table = catalog.dataset("t.quotes", field=Quote.into_field()).get_or_create_table()

    assert catalog.catalog.properties["py-io-impl"] == TRACKED_FILE_IO
    assert isinstance(table.io, TrackedFileIO)
    assert isinstance(table.io.delegate, CustomArrowFileIO)


def test_the_catalog_is_loaded_once(catalog: IcebergCatalog) -> None:
    assert catalog.catalog is catalog.catalog


def test_concurrent_first_access_loads_one_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two first readers share the handle initialized under the location guard."""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    import pyiceberg.catalog

    second_waiting = threading.Event()
    loaded = 0

    class Guard:
        def __init__(self) -> None:
            self.lock = threading.RLock()
            self.attempts = 0

        def __enter__(self) -> None:
            self.attempts += 1
            if self.attempts == 2:
                second_waiting.set()
            self.lock.acquire()

        def __exit__(self, *_args: object) -> None:
            self.lock.release()

    class Opened:
        def close(self) -> None:
            pass

    opened = Opened()

    def load(*_args: object, **_kwargs: object) -> Opened:
        nonlocal loaded
        loaded += 1
        if loaded == 1:
            assert second_waiting.wait(timeout=5)
        return opened

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", load)
    catalog = IcebergCatalog()
    catalog.__dict__["_location_guard"] = Guard()

    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(lambda: catalog.catalog)
        second = workers.submit(lambda: catalog.catalog)
        handles = first.result(timeout=5), second.result(timeout=5)

    assert handles == (opened, opened)
    assert loaded == 1


def test_close_is_lazy_and_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Teardown must neither open a catalog nor close one twice."""
    import pyiceberg.catalog

    loaded = 0

    class Opened:
        closed = 0

        def close(self) -> None:
            self.closed += 1

    opened = Opened()

    def load(*_args, **_kwargs) -> Opened:
        nonlocal loaded
        loaded += 1
        return opened

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", load)
    catalog = IcebergCatalog()
    catalog.close()
    assert loaded == 0

    assert catalog.catalog is opened
    catalog.close()
    catalog.close()
    assert (loaded, opened.closed) == (1, 1)


def test_a_dataset_only_closes_the_catalog_it_owns(monkeypatch: pytest.MonkeyPatch) -> None:
    """A catalog sweep lends one connection to every dataset it creates."""
    catalog = IcebergCatalog()
    closed = 0

    def close() -> None:
        nonlocal closed
        closed += 1

    monkeypatch.setattr(catalog, "close", close)
    shared = catalog.dataset("trading.quotes", field=Quote.into_field())
    shared.close()
    assert closed == 0

    owned = IcebergDataset(name="quotes", namespace="trading", field=Quote.into_field())
    owned.__dict__["store"] = catalog
    owned.__dict__["_owns_store"] = True
    owned.close()
    owned.close()
    assert closed == 1


# -- namespaces -------------------------------------------------------------


def test_namespaces_are_created_listed_and_dropped(catalog: IcebergCatalog) -> None:
    assert catalog.namespaces() == []
    catalog.create_namespace("trading")
    assert catalog.namespaces() == ["trading"]
    assert catalog.namespace_exists("trading")
    catalog.drop_namespace("trading")
    assert catalog.namespaces() == []


def test_creating_a_namespace_twice_is_not_an_error(catalog: IcebergCatalog) -> None:
    catalog.create_namespace("trading")
    catalog.create_namespace("trading")
    assert catalog.namespaces() == ["trading"]


def test_creating_a_namespace_twice_can_be_refused(catalog: IcebergCatalog) -> None:
    from pyiceberg.exceptions import NamespaceAlreadyExistsError

    catalog.create_namespace("trading")
    with pytest.raises(NamespaceAlreadyExistsError):
        catalog.create_namespace("trading", exists_ok=False)


def test_dropping_a_namespace_that_is_not_there_is_not_an_error(catalog: IcebergCatalog) -> None:
    catalog.drop_namespace("absent")


def test_namespace_properties_round_trip(catalog: IcebergCatalog) -> None:
    space = catalog.create_namespace("trading", {"owner": "desk"})
    assert space.properties["owner"] == "desk"
    space.update_properties({"owner": "risk"})
    assert space.properties["owner"] == "risk"


def test_a_namespace_hands_out_its_own_datasets(catalog: IcebergCatalog) -> None:
    space = catalog.create_namespace("trading")
    dataset = space.dataset("quotes", field=Quote.into_field())
    assert isinstance(dataset, IcebergDataset)
    assert dataset.name == "quotes"
    assert dataset.identifier == "trading.quotes"
    assert dataset.namespace == "trading"
    assert dataset.field.name == "quotes"
    assert dataset.store is catalog
    assert dataset.catalog is catalog.catalog


@pytest.mark.parametrize(
    ("name", "namespace", "message"),
    [
        ("", "trading", "name must be non-empty and unqualified"),
        ("trading.quotes", "trading", "name must be non-empty and unqualified"),
        ("quotes", "", "namespace must be non-empty"),
        ("quotes", "trading..eu", "namespace must be non-empty"),
    ],
)
def test_a_direct_dataset_requires_explicit_coordinates(
    name: str, namespace: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        IcebergDataset(name=name, namespace=namespace, field=Quote.into_field())


# -- tables -----------------------------------------------------------------


def test_tables_are_listed_per_namespace_and_across_them(catalog: IcebergCatalog) -> None:
    catalog.create_namespace("trading")
    catalog.create_namespace("risk")
    catalog.dataset("trading.quotes", field=Quote.into_field()).create_with()
    catalog.dataset("risk.limits", field=Quote.into_field()).create_with()
    assert catalog.tables("trading") == ["trading.quotes"]
    assert sorted(catalog.tables()) == ["risk.limits", "trading.quotes"]


def test_tables_reach_nested_namespaces(catalog: IcebergCatalog) -> None:
    """`list_namespaces` is one level deep, so a sweep silently skipped the rest.

    `for dataset in catalog.datasets(): dataset.optimize()` never touched
    `trading.eu.paris.quotes`, and reported no skip.
    """
    for name in ("ops.quotes", "trading.quotes", "trading.eu.quotes", "trading.eu.paris.quotes"):
        catalog.dataset(name, field=Quote.into_field()).create_with()
    assert sorted(catalog.tables()) == [
        "ops.quotes",
        "trading.eu.paris.quotes",
        "trading.eu.quotes",
        "trading.quotes",
    ]
    assert catalog.tables("trading") == ["trading.quotes"], "one namespace is still one"
    assert "trading.eu" in catalog.namespaces("trading")
    assert sorted(catalog.namespaces(recursive=True)) == [
        "ops",
        "trading",
        "trading.eu",
        "trading.eu.paris",
    ]


def test_a_sweep_loads_one_catalog(catalog: IcebergCatalog) -> None:
    """Loading a pyiceberg catalog builds an engine, or asks a REST server."""
    import pyiceberg.catalog

    for index in range(6):
        catalog.dataset(f"trading.q{index}", field=Quote.into_field()).create_with()
    loaded = 0
    original = pyiceberg.catalog.load_catalog

    def counted(*args, **kwargs):
        nonlocal loaded
        loaded += 1
        return original(*args, **kwargs)

    pyiceberg.catalog.load_catalog = counted
    try:
        names = [dataset.name for dataset in catalog.datasets()]
    finally:
        pyiceberg.catalog.load_catalog = original
    assert len(names) == 6
    assert loaded == 0, "the catalog it came from is the catalog it uses"


def test_a_table_is_dropped_and_purged(catalog: IcebergCatalog) -> None:
    dataset = catalog.dataset("trading.quotes", field=Quote.into_field())
    dataset.create_with()
    assert catalog.table_exists("trading.quotes")
    catalog.drop_table("trading.quotes")
    assert not catalog.table_exists("trading.quotes")
    catalog.drop_table("trading.quotes"), "dropping what is gone is not an error"


def test_a_table_is_renamed(catalog: IcebergCatalog) -> None:
    catalog.dataset("trading.quotes", field=Quote.into_field()).create_with()
    catalog.rename_table("trading.quotes", "trading.ticks")
    assert catalog.tables("trading") == ["trading.ticks"]


def test_every_table_comes_back_as_a_dataset(catalog: IcebergCatalog) -> None:
    catalog.dataset("trading.quotes", field=Quote.into_field()).create_with()
    catalog.dataset("trading.ticks", field=Quote.into_field()).create_with()
    found = {dataset.name for dataset in catalog.datasets("trading")}
    assert found == {"quotes", "ticks"}
    for dataset in catalog.datasets("trading"):
        assert [member.name for member in dataset.into_struct_field()] == ["symbol", "size"]


def test_the_catalog_is_a_document(catalog: IcebergCatalog) -> None:
    assert set(catalog.into_dict()) == {"name", "properties"}
    rebuilt = IcebergCatalog.from_json(catalog.into_json())
    assert (rebuilt.name, rebuilt.properties) == (
        catalog.name,
        catalog.properties,
    )


def test_a_catalog_name_is_explicit_and_nonempty() -> None:
    with pytest.raises(TypeError, match="must be a string"):
        IcebergCatalog(name=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-empty"):
        IcebergCatalog(name="")


def test_maintenance_reaches_the_store_the_catalog_was_configured_with() -> None:
    """Maintenance reuses the FileIO's configured endpoint and credentials."""
    from rekep.iceberg.dataset import _store_of

    configured = pyarrow.fs.SubTreeFileSystem("/warehouse", pyarrow.fs.LocalFileSystem())
    file_io = IcebergFileIO({"s3.endpoint": "http://minio:9000"})
    file_io.fs_by_scheme = lambda _scheme, _netloc: configured

    class Table:
        io = file_io

    filesystem, base = _store_of(Table(), "s3://bucket/wh/db/t/data")

    assert filesystem is configured
    assert base == "bucket/wh/db/t/data"

    class Bare:
        pass

    class Unbacked:
        io = Bare()

    with pytest.raises(TypeError, match="cannot expose its configured Arrow store"):
        _store_of(Unbacked(), "s3://bucket/wh")


def test_the_sweep_deletes_through_yggdryl_and_tolerates_absence(tmp_path: Path) -> None:
    from rekep.iceberg.dataset import IcebergDataset

    orphan = tmp_path / "orphan.avro"
    orphan.write_bytes(b"old")
    dataset = object.__new__(IcebergDataset)
    found = [(pyarrow.fs.LocalFileSystem(), os.fspath(orphan), orphan.as_uri(), 3)]
    dataset._sweep(found)
    dataset._sweep(found)

    assert not orphan.exists()


def test_the_sweep_propagates_nonabsence_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    import yggdryl

    from rekep.iceberg.dataset import IcebergDataset

    class Refused:
        @staticmethod
        def unlink() -> None:
            raise PermissionError("refused")

    class IOBase:
        @staticmethod
        def from_fs(_filesystem: object, _path: str) -> Refused:
            return Refused()

    monkeypatch.setattr(yggdryl, "IOBase", IOBase)
    dataset = object.__new__(IcebergDataset)
    with pytest.raises(PermissionError, match="refused"):
        dataset._sweep([(object(), "orphan.avro", "mock://bound/orphan.avro", 3)])


def test_an_unknown_mtime_is_spared_by_the_orphan_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import datetime

    from rekep.iceberg import dataset as module
    from rekep.iceberg.dataset import IcebergDataset

    class FileSystem:
        @staticmethod
        def get_file_info(_selector: pyarrow.fs.FileSelector) -> list[pyarrow.fs.FileInfo]:
            return [
                pyarrow.fs.FileInfo(
                    "root/uncommitted.parquet",
                    pyarrow.fs.FileType.File,
                    size=4,
                )
            ]

    filesystem = FileSystem()
    monkeypatch.setattr(module, "_store_of", lambda _table, _directory: (filesystem, "root"))
    dataset = object.__new__(IcebergDataset)
    dataset.__dict__.update(
        iceberg_table=object(),
        # The sweep asks its catalog who owns the files before it lists any.
        store=IcebergCatalog(properties={"type": "sql"}),
        refresh=lambda: dataset,
        _live=lambda _table: (set(), set()),
        _data_path=lambda _table: "file:///root",
    )

    assert dataset._orphans(datetime.timedelta(days=3), metadata=False) == []
    assert dataset._orphans(datetime.timedelta(0), metadata=False)[0][1] == (
        "root/uncommitted.parquet"
    )


def test_a_table_bucket_keeps_its_own_files(caplog: pytest.LogCaptureFixture) -> None:
    """S3 Tables owns the files under a table; a sweep here settles nothing."""
    import logging

    from rekep.iceberg.dataset import IcebergDataset

    dataset = IcebergDataset(
        name="messages",
        namespace="logs",
        field=Quote.into_field(),
        catalog_name="rekep",
        catalog_properties={"type": "s3tables", "warehouse": TABLE_BUCKET},
    )

    with caplog.at_level(logging.INFO, logger="rekep.iceberg.dataset"):
        # Neither the catalog nor the table is opened to answer this.
        assert dataset.orphan_files() == []
    assert TABLE_BUCKET in caplog.text


@pytest.mark.skipif(os.name != "nt", reason="Windows path normalization")
def test_native_file_io_resolves_windows_paths_and_file_uris(tmp_path: Path) -> None:
    target = tmp_path / "metadata.json"

    assert Path(IcebergFileIO.parse_location(os.fspath(target))[2]) == target
    assert Path(IcebergFileIO.parse_location(target.as_uri())[2]) == target
