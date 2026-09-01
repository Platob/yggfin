import bz2
import datetime
import gzip
import hashlib
import re
from pathlib import Path

import pyarrow
import pyarrow.fs
import pytest
from fsspec.implementations.memory import MemoryFile, MemoryFileSystem

import rekep.text.entries as entries
import rekep.text.text_file as text_file_module
from rekep import Dataset, Field, FixCodec, FixMsg, FixRegistry, Message, txhash
from rekep.enums import Direction, EventType, Plugin, Protocol
from rekep.filesystems import ArrowFile
from rekep.market.event import HOUR, SECOND, unix_partition_arrow
from rekep.market.identity import HASH
from rekep.text import HEADER_PATTERN, TextFile
from rekep.text.text_file import _local_micros
from rekep.times import unix_of

SAMPLE = Path(__file__).parent.parent / "data" / "app_sample.txt"
SAMPLE_BYTES = SAMPLE.read_bytes()

#: Expectations are derived from the sample, then pinned, so that a regression
#: in HEADER_PATTERN cannot quietly move both sides of an assertion together.
RECORDS = [line for line in SAMPLE_BYTES.split(b"\n") if HEADER_PATTERN.match(line)]
CONTINUATIONS = [
    line for line in SAMPLE_BYTES.split(b"\n") if line and not HEADER_PATTERN.match(line)
]
EXPECTED_RECORDS = 24
EXPECTED_CONTINUATIONS = 4
FIRST_UNIX = 1_786_665_901_147_250_000  # 2026-08-14 00:05:01.147_250 as ns


def test_sample_shape_is_what_the_tests_assume() -> None:
    assert len(RECORDS) == EXPECTED_RECORDS
    assert len(CONTINUATIONS) == EXPECTED_CONTINUATIONS


def test_sample_has_no_retained_fix_payload_fields() -> None:
    registry = FixRegistry.from_builtin()
    codec = FixCodec(registry=registry)
    with TextFile.from_path(
        SAMPLE,
        msg_type_event_types=registry.msg_type_event_types(),
        protocol_rules=codec.rules,
    ) as log:
        messages = log.read_arrow_table()
    parsed = pyarrow.Table.from_batches(
        [FixMsg.from_message_batch(batch, codec) for batch in messages.to_batches()]
    )

    assert parsed.num_rows == EXPECTED_RECORDS
    assert parsed.column("entries").to_pylist() == [None] * EXPECTED_RECORDS
    assert parsed.column("unmap").to_pylist() == [None] * EXPECTED_RECORDS


@pytest.fixture
def plain(tmp_path: Path) -> Path:
    path = tmp_path / "app.txt"
    path.write_bytes(SAMPLE_BYTES)
    return path


@pytest.fixture
def gzipped(tmp_path: Path) -> Path:
    """Written by stdlib gzip, so this proves interop rather than a round-trip."""
    path = tmp_path / "app.txt.gz"
    path.write_bytes(gzip.compress(SAMPLE_BYTES))
    return path


@pytest.fixture
def zstandard(tmp_path: Path) -> Path:
    path = tmp_path / "app.txt.zst"
    with pyarrow.CompressedOutputStream(str(path), "zstd") as out:
        out.write(SAMPLE_BYTES)
    return path


#: Two FIX-looking payloads and one ordinary payload. Text ingestion keeps all
#: three opaque; the protocol parser owns every tag-level interpretation.
WIRE = (
    "2026-08-14 00:05:01.147_250 [t] [d] (INFO) "
    "8=FIX.4.2|9=176|35=D|34=7|49=BUY|50=DESK1|56=XPAR|115=CLIENTA|43=Y|"
    "52=20260814-09:30:00.123456789|11=ORD-1|55=TTF|54=1|38=1200|44=41.25|"
    "60=20260814-09:29:59.5|58=ok|10=203|\n"
    "2026-08-14 00:05:01.147_251 [t] [d] (INFO) "
    "8=FIX.4.4|35=AB|34=8|49=BUY|56=XPAR|555=2|600=TTF|55=SPREAD|555=2|55=OTHER|10=011|\n"
    "2026-08-14 00:05:01.147_252 [t] [d] (INFO) heartbeat\n"
)


@pytest.fixture
def wire(tmp_path: Path) -> Path:
    path = tmp_path / "wire.txt"
    path.write_text(WIRE)
    return path


# -- header pattern ---------------------------------------------------------


def test_header_pattern_splits_a_row() -> None:
    match = HEADER_PATTERN.match(RECORDS[0])
    assert match is not None
    assert match["timestamp"] == b"2026-08-14 00:05:01.147_250"
    assert match["threadname"] == b"250-e7256476:9effef3e6a:72505"
    assert match["plugin"] == b"OMSSales_Enrichment"
    assert match["level"] == b"DEBUG"
    assert match["body"].startswith(b"-> [5] {trade")


def test_header_pattern_tolerates_a_missing_level() -> None:
    (row,) = [r for r in RECORDS if HEADER_PATTERN.match(r)["level"] is None]
    assert HEADER_PATTERN.match(row)["body"] == b"no level printed by this driver"


def test_xmlapi_header_classifies_receiving_xml_and_keeps_nested_entries(tmp_path: Path) -> None:
    path = tmp_path / "xmlapi.txt"
    body = (
        'Receiving: <event id="20260828-220500-041-00" type="orderdelta" '
        'timestamp="20260828-22:05:00.041190"><action '
        'id="IRI20260828-220500-041-000" userid="uliris_pco" '
        'type="orderdelta.update"><orderdelta '
        'id="BCGF42#####dbi;GB00BN7SWP63_XLON_GBX#sell#open#GBP###uliris_pco#false#false#false" '
        'clientid="BCGF42" instrumentid="dbi;GB00BN7SWP63_XLON_GBX" '
        'exchangeid="XLON" side="sell" quantity="-400"/>'
        "</action></event>"
    )
    path.write_text(f"2026-08-29 00:05:00.042_525 [135] [XmlApi] (INFO) {body}\n")

    with TextFile.from_path(
        path,
        plugin_keys={"xmlapi": {"clientid": "ClOrdID"}},
    ) as log:
        table = log.read_arrow_table()

    assert table.column("plugin").to_pylist() == [Plugin.from_str("XmlApi").into_stored()]
    assert table.column("body").to_pylist() == [body.encode()]
    assert Protocol.from_int(table.column("protocol")[0].as_py()) is Protocol.XML
    assert Direction.from_int(table.column("direction")[0].as_py()) is Direction.RECV
    entries = table.column("entries")[0].as_py()
    assert (
        "event[0].action[0].orderdelta[0]",
        "ClOrdID",
        "BCGF42",
    ) in [(entry["comp"], entry["key"], entry["value"]) for entry in entries]
    assert (entries[-1]["comp"], entries[-1]["key"], entries[-1]["value"]) == (
        "event[0].action[0].orderdelta[0]",
        "quantity",
        "-400",
    )


def test_file_applies_plugin_keys_and_null_values_before_storing_entries(tmp_path: Path) -> None:
    path = tmp_path / "bridge.txt"
    path.write_text(
        "2026-08-29 00:05:00.042_525 [135] [Bridge] (INFO) TYPE=D|CLIENT=A-1|EMPTY=NONE|\n"
    )

    with TextFile.from_path(
        path,
        msg_type_event_types={"D": EventType.ORDER},
        plugin_keys={"bridge": {"TYPE": "MsgType", "CLIENT": "ClOrdID"}},
        null_values=["none"],
    ) as log:
        table = log.read_arrow_table()

    assert table.column("msgtype").to_pylist() == ["D"]
    assert table.column("eventtype").to_pylist() == [int(EventType.ORDER)]
    assert [(entry["key"], entry["value"]) for entry in table.column("entries")[0].as_py()] == [
        ("ClOrdID", "A-1")
    ]


@pytest.mark.parametrize(
    "line",
    [
        b"java.lang.IllegalStateException: no binding for token 'venue.mic'",
        b"\tat com.example.objkey.TagWrapper.evaluate(TagWrapper.java:214)",
        b"",
        b"not a log line at all",
    ],
)
def test_header_pattern_rejects_continuations(line: bytes) -> None:
    assert HEADER_PATTERN.match(line) is None


# -- millis, or micros ------------------------------------------------------


#: Every fractional-second spelling the header admits, and the nanosecond it
#: means. Derived once, pinned here, because "millis read as micros" produces a
#: perfectly plausible instant 147 milliseconds off.
FRACTIONS = {
    "2026-08-14 00:05:01.147_250": 1_786_665_901_147_250_000,
    "2026-08-14 00:05:01.147250": 1_786_665_901_147_250_000,
    "2026-08-14 00:05:01,147,250": 1_786_665_901_147_250_000,
    "2026-08-14 00:05:01.147": 1_786_665_901_147_000_000,
    "2026-08-14 00:05:01,147": 1_786_665_901_147_000_000,
}


@pytest.mark.parametrize(("stamp", "unix"), FRACTIONS.items(), ids=lambda v: str(v)[:28])
def test_a_stamp_lands_on_the_instant_it_spells(tmp_path: Path, stamp: str, unix: int) -> None:
    path = tmp_path / "one.txt"
    path.write_text(f"{stamp} [t] [d] (INFO) hello\n")
    with TextFile.from_path(path) as log:
        assert log.read_arrow_table().column("unix").to_pylist() == [unix]


def test_millis_are_padded_and_never_right_aligned(tmp_path: Path) -> None:
    """`147` is 147 milliseconds. Read as `000147` it is 147 ms early, and looks fine."""
    path = tmp_path / "millis.txt"
    path.write_text("2026-08-14 00:05:01.147 [t] [d] (INFO) hello\n")
    with TextFile.from_path(path) as log:
        (unix,) = log.read_arrow_table().column("unix").to_pylist()
    assert unix % 1_000_000_000 == 147_000_000
    assert unix != 1_786_665_901_000_147_000


def test_a_batch_mixing_both_widths_reads_every_row(tmp_path: Path) -> None:
    """The slicing path is per batch, so a mixed batch takes the read path."""
    path = tmp_path / "mixed.txt"
    path.write_text("".join(f"{stamp} [t] [d] (INFO) hello\n" for stamp in FRACTIONS))
    with TextFile.from_path(path) as log:
        assert log.read_arrow_table().column("unix").to_pylist() == list(FRACTIONS.values())


def test_a_written_batch_reparses_to_the_same_instants(tmp_path: Path) -> None:
    """One canonical spelling out, and it has to read back as what went in."""
    source = tmp_path / "mixed.txt"
    source.write_text("".join(f"{stamp} [t] [d] (INFO) hello\n" for stamp in FRACTIONS))
    with TextFile.from_path(source) as log:
        rows = log.read_arrow_table()
    copy = tmp_path / "copy.txt"
    out = TextFile.from_path(copy)
    out.append_arrow(rows)
    with TextFile.from_path(copy) as again:
        written = again.read_arrow_table()
    assert written.column("unix").to_pylist() == rows.column("unix").to_pylist()
    assert {line.split(b" [")[0][20:] for line in copy.read_bytes().splitlines()} == {
        b"147_250",
        b"147_000",
    }, "micros, with the separator, whichever spelling came in"


def test_a_fraction_is_one_to_nine_digits_or_none_at_all() -> None:
    """The fraction is one group of up to nine digits, in every shape.

    Nanoseconds are a spelling: a logger that prints them is not writing a
    continuation line. What is still not a header is a fraction wider than the
    finest instant there is.
    """
    for stamp in (
        b"2026-08-14 00:05:01.1",
        b"2026-08-14 00:05:01.147",
        b"2026-08-14 00:05:01.147250",
        b"2026-08-14 00:05:01.147_250",
        b"2026-08-14 00:05:01.147250123",
        b"2026-08-14 00:05:01",
    ):
        assert HEADER_PATTERN.match(stamp + b" [t] [d] (INFO) x") is not None, stamp
    assert HEADER_PATTERN.match(b"2026-08-14 00:05:01.1472501234 [t] [d] (INFO) x") is None


def test_the_other_two_shapes_open_a_header_too() -> None:
    """FIX's own spelling and a compact one, beside the rendered ISO."""
    for stamp in (
        b"20260824-10:00:01.123",
        b"20260824-10:00:01",
        b"20260824100001123",
        b"20260824100001",
        b"20260824100001123456789",
    ):
        found = HEADER_PATTERN.match(stamp + b" [t] [d] (INFO) x")
        assert found is not None, stamp
        assert found.group("timestamp") == stamp
        assert found.group("body") == b"x"


def test_two_shapes_of_one_width_in_one_batch_read_as_themselves() -> None:
    """Three widths are shared by two shapes, and a width alone must not decide.

    `20260824-10:00:01` is a FIX stamp and `20260824100001123` a compact one,
    and both are seventeen characters. Sliced as the wrong shape either reads
    into a *plausible* instant off by hours -- so a batch carrying both is
    grouped rather than sliced as whichever shape was tried last.
    """
    shared = (
        (b"20260824-10:00:01", b"20260824100001123"),
        (b"2026-08-14 00:05:01.147", b"20260814000501147250123"),
        (b"2026-08-14 00:05:01.147_250", b"20260814-00:05:01.147250123"),
    )
    for pair in shared:
        assert len(pair[0]) == len(pair[1]), pair
        found = _local_micros(list(pair)).to_pylist()
        for spelled, got in zip(pair, found, strict=True):
            assert unix_of(got) == unix_of(spelled.decode()), (spelled, got)


def test_the_reader_and_the_window_accept_the_same_spellings() -> None:
    """One declaration, two executions: `times` reads a window, this reads a column.

    A shape one accepted and the other did not would be a stamp a window
    could not name -- or a header the parser folded into the line above it.
    """
    for stamp in (
        b"2026-08-14 00:05:01.147",
        b"2026-08-14 00:05:01.147_250",
        b"2026-08-14 00:05:01",
        b"20260824-10:00:01.123",
        b"20260824100001123",
        b"20260824100001",
    ):
        text = stamp.decode()
        assert HEADER_PATTERN.match(stamp + b" [t] [d] x") is not None, text
        found = _local_micros([stamp])[0].as_py()
        assert unix_of(found) == unix_of(text), text


# -- construction -----------------------------------------------------------


def test_post_init_builds_the_filesystem_and_rewrites_url(plain: Path) -> None:
    log = TextFile(url=plain.as_uri())
    assert isinstance(log.filesystem, pyarrow.fs.LocalFileSystem)
    assert log.url != plain.as_uri(), "url should be rewritten as a filesystem path"
    assert log.url.endswith("app.txt")
    assert "://" not in log.url
    log.close()


def test_supplied_filesystem_leaves_url_alone(plain: Path) -> None:
    filesystem = pyarrow.fs.LocalFileSystem()
    log = TextFile(url=str(plain), filesystem=filesystem)
    assert log.filesystem is filesystem
    assert log.url == str(plain)
    with log:
        assert log.read() == SAMPLE_BYTES


def test_from_url(plain: Path) -> None:
    with TextFile.from_url(plain.as_uri()) as log:
        assert log.read() == SAMPLE_BYTES


def test_from_path_accepts_a_relative_path(plain: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(plain.parent)
    with TextFile.from_path("app.txt") as log:
        assert log.read() == SAMPLE_BYTES


def test_construction_is_only_ever_a_classmethod() -> None:
    """There is no module-level factory to drift out of step with the class."""
    import rekep.text.text_file as module

    assert not hasattr(module, "text_file")
    assert {"from_", "from_url", "from_path"} <= set(dir(TextFile))


# -- generic dispatch -------------------------------------------------------


def test_from_redirects_on_the_source_type(plain: Path) -> None:
    assert TextFile.redirect_of(plain) == "path"
    assert TextFile.redirect_of(plain.as_uri()) == "url"
    with TextFile.from_(plain) as from_path, TextFile.from_(plain.as_uri()) as from_url:
        assert from_path.url == from_url.url


@pytest.mark.parametrize(
    ("requested", "stem"),
    [
        (pyarrow.Table, "arrow_table"),
        (pyarrow.RecordBatchReader, "arrow_reader"),
        (pyarrow.RecordBatch, "arrow_batches"),
    ],
)
def test_into_redirects_on_the_requested_type(plain: Path, requested: type, stem: str) -> None:
    assert TextFile.redirect_of(requested) == stem
    with TextFile.from_(plain) as log:
        assert log.into_(requested) is not None


def test_into_table_via_dispatch_matches_the_named_method(plain: Path) -> None:
    with TextFile.from_(plain) as dispatched, TextFile.from_(plain) as named:
        assert dispatched.into_(pyarrow.Table).equals(named.into_arrow_table())


def test_dispatch_refuses_what_it_cannot_infer(plain: Path) -> None:
    with TextFile.from_(plain) as log, pytest.raises(TypeError, match="cannot infer"):
        log.into_(object())


# -- codec detection --------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("app.txt", None),
        ("app.log", None),
        ("app", None),
        ("app.txt.gz", "gzip"),
        ("app.txt.zst", "zstd"),
        ("app.txt.bz2", "bz2"),
        ("app.txt.lz4", "lz4"),
    ],
)
def test_codec_detected_from_extension(tmp_path: Path, name: str, expected: str | None) -> None:
    assert TextFile(url=tmp_path.joinpath(name).as_uri())._codec == expected


@pytest.mark.parametrize("fixture", ["plain", "gzipped", "zstandard"])
def test_reads_whole_log_whatever_the_encoding(
    request: pytest.FixtureRequest, fixture: str
) -> None:
    path: Path = request.getfixturevalue(fixture)
    with TextFile(url=path.as_uri()) as log:
        assert log.read() == SAMPLE_BYTES


def test_local_compressed_and_remote_plain_reads_do_not_spill(
    gzipped: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local = TextFile.from_path(gzipped)
    assert local.fileio.spill(temporary=True) is local.fileio
    assert local.read() == SAMPLE_BYTES

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("a remote plain source must stream directly")

    monkeypatch.setattr(ArrowFile, "spill", forbidden)
    store = pyarrow.fs._MockFileSystem()
    store.create_dir("captures")
    with store.open_output_stream("captures/app.txt", compression=None) as stream:
        stream.write(SAMPLE_BYTES)
    assert TextFile(url="captures/app.txt", filesystem=store).read() == SAMPLE_BYTES


@pytest.mark.parametrize(
    ("suffix", "codec"),
    [("gz", "gzip"), ("bz2", "bz2"), ("zst", "zstd")],
)
def test_remote_compressed_logs_stream_in_bounded_reads_without_spilling(
    suffix: str, codec: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = b"".join(
        (
            f"2026-08-14 00:05:{index % 60:02d}.{index % 1000:03d} "
            f"[t] [M] (INFO) {hashlib.sha256(str(index).encode()).hexdigest()}\n"
        ).encode()
        for index in range(5_000)
    )
    if codec == "gzip":
        payload = gzip.compress(raw)
    elif codec == "bz2":
        payload = bz2.compress(raw)
    else:
        payload = pyarrow.Codec(codec).compress(raw)

    memory = MemoryFileSystem()
    path = f"/captures/direct-{codec}.log.{suffix}"
    memory.pipe(path, payload)
    filesystem = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(memory))
    reads: list[int] = []
    original_read = MemoryFile.read

    def tracked_read(opened, size=-1):  # noqa: ANN001, ANN202
        reads.append(size)
        return original_read(opened, size)

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("the default compressed path must not spill")

    monkeypatch.setattr(MemoryFile, "read", tracked_read)
    monkeypatch.setattr(ArrowFile, "spill", forbidden)
    with TextFile(url=path, filesystem=filesystem) as log:
        batches = list(
            log.into_arrow_batches(
                batch_row_size=257,
                batch_byte_size=1 << 20,
                read_byte_size=1 << 10,
            )
        )

    assert sum(batch.num_rows for batch in batches) == 5_000
    assert max(batch.num_rows for batch in batches) <= 257
    assert reads and all(0 < size <= 1 << 16 for size in reads)


def test_a_compressed_log_on_a_local_subtree_streams_without_spilling(tmp_path: Path) -> None:
    root = tmp_path / "mounted"
    root.mkdir()
    path = root / "app.txt.gz"
    path.write_bytes(gzip.compress(SAMPLE_BYTES))
    filesystem = pyarrow.fs.SubTreeFileSystem(root.as_posix(), pyarrow.fs.LocalFileSystem())

    log = TextFile(url="app.txt.gz", filesystem=filesystem)
    cache = tmp_path / "unused"
    assert log.fileio.spill(local=cache, temporary=True) is log.fileio
    assert not cache.exists()
    assert log.read() == SAMPLE_BYTES


def test_a_remote_compressed_log_spills_raw_bytes_and_refreshes_when_it_grows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = pyarrow.fs._MockFileSystem()
    store.create_dir("captures")
    remote = "captures/app.txt.gz"
    first_payload = gzip.compress(SAMPLE_BYTES)
    with store.open_output_stream(remote, compression=None) as stream:
        stream.write(first_payload)

    spilled: list[str] = []
    original = ArrowFile.spill

    def into_test_cache(self, local=None, *, temporary=False):  # noqa: ANN001, ANN202
        materialized = original(self, tmp_path / "spill", temporary=temporary)
        assert materialized is not None and materialized.location is not None
        spilled.append(materialized.location)
        return materialized

    monkeypatch.setattr(ArrowFile, "spill", into_test_cache)
    log = TextFile(url=remote, filesystem=store, spill=True)
    first = log.read_arrow_table()

    assert first.num_rows == EXPECTED_RECORDS
    assert set(first.column("sourceurl").to_pylist()) == {remote}
    target = Path(spilled[0])
    assert not target.exists(), "normal reader exhaustion purges its compressed spill"

    added = b"2026-08-14 00:06:00.000 [t] [M] (INFO) added\n"
    second_payload = gzip.compress(SAMPLE_BYTES + added)
    assert len(second_payload) != len(first_payload)
    with store.open_output_stream(remote, compression=None) as stream:
        stream.write(second_payload)
    second = log.read_arrow_table()

    assert second.num_rows == EXPECTED_RECORDS + 1
    assert second.column("body").cast(pyarrow.string())[-1].as_py() == "added"
    assert spilled[1] != spilled[0], "temporary readers never share deletion ownership"
    assert not Path(spilled[1]).exists()


def test_a_missing_remote_compressed_log_is_lazy_and_never_materialized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = pyarrow.fs._MockFileSystem()
    calls = 0
    original = ArrowFile.spill

    def into_test_cache(self, local=None, *, temporary=False):  # noqa: ANN001, ANN202
        nonlocal calls
        calls += 1
        return original(self, tmp_path / "spill", temporary=temporary)

    monkeypatch.setattr(ArrowFile, "spill", into_test_cache)
    log = TextFile(url="captures/missing.txt.gz", filesystem=store, spill=True)

    assert calls == 0
    assert not (tmp_path / "spill").exists(), "construction performs no I/O"
    with pytest.raises(FileNotFoundError, match="missing.txt.gz"):
        log.read(1)
    assert calls == 1
    assert not (tmp_path / "spill").exists()


# -- schema -----------------------------------------------------------------


#: The standard header the raw stage lifts out of `entries` into columns of
#: its own, in the order a message re-emits them. `CheckSum` is deliberately
#: not among them: it is the boundary every lift is measured against, so it
#: stays an entry.
SESSION_COLUMNS = [
    "beginstring",
    "bodylength",
    "msgtype",
    "sendercompid",
    "sendersubid",
    "senderlocationid",
    "targetcompid",
    "targetsubid",
    "targetlocationid",
    "onbehalfofcompid",
    "onbehalfofsubid",
    "onbehalfoflocationid",
    "delivertocompid",
    "delivertosubid",
    "delivertolocationid",
    "msgseqnum",
    "lastmsgseqnumprocessed",
    "possdupflag",
    "possresend",
    "sendingtime",
    "origsendingtime",
    "onbehalfofsendingtime",
    "applverid",
    "cstmapplverid",
    "applextid",
    "messageencoding",
    "securedatalen",
    "securedata",
    "signaturelength",
    "signature",
]

MESSAGE_COLUMNS = [
    "sourceurl",
    "sourcerownum",
    "threadname",
    "body",
    "protocol",
    *SESSION_COLUMNS,
    "entries",
    "direction",
]

#: Names that would only exist if a protocol had read the payload: a field the
#: raw stage never lifts, and the snake spellings a FIX schema renders. The
#: seven the header does lift are absent here because they now *are* raw
#: columns -- `test_schema` pins instead that they carry no `fix:` reading.
FIX_COLUMNS = {
    "CheckSum",
    "Symbol",
    "begin_string",
    "msg_type",
    "sender_comp_id",
    "target_comp_id",
    "symbol",
}


def test_schema(plain: Path) -> None:
    schema = TextFile(url=plain.as_uri()).schema
    assert schema.equals(Message.into_field().into_arrow_schema())
    assert schema.names[:3] == ["unix", "unixpartition", "eventtype"], "the envelope leads"
    assert schema.names[-len(MESSAGE_COLUMNS) :] == MESSAGE_COLUMNS
    assert FIX_COLUMNS.isdisjoint(schema.names)
    for name in SESSION_COLUMNS:
        field = schema.field(name)
        assert field.type == pyarrow.string(), f"{name} is the text the payload spelled"
        protocol = {key for key in field.metadata or {} if key.startswith(b"fix:")}
        assert protocol == {b"fix:name"}, (
            f"{name} is lifted by syntax, so it says what it is called and nothing more"
        )
    assert schema.field("unix").type == pyarrow.int64()
    assert schema.field("unixpartition").type == pyarrow.int32()
    assert schema.field("hash").type == HASH
    assert schema.field("vhash").type == pyarrow.int64()
    assert schema.field("xhash").type == HASH
    assert schema.field("eventtype").type == pyarrow.int64()
    assert schema.field("protocol").type == Protocol.into_storage_type()
    assert schema.field("body").type == pyarrow.binary()


def test_fix_looking_payloads_keep_only_syntax_level_arguments(wire: Path) -> None:
    table = TextFile.from_path(
        wire,
        msg_type_event_types=FixRegistry.from_builtin().msg_type_event_types(),
    ).read_arrow_table()
    payloads = [line.split(" (INFO) ", 1)[1] for line in WIRE.splitlines()]

    assert table.schema.names == Message.into_field().names
    assert table.column("body").cast(pyarrow.string()).to_pylist() == payloads
    assert table.column("msgtype").to_pylist() == ["D", "AB", None]
    assert table.column("eventtype").to_pylist() == [
        int(EventType.ORDER),
        int(EventType.MISC),
        int(EventType.MISC),
    ]
    assert table.column("lastmkt").to_pylist() == [None] * 3
    assert table.column("code").to_pylist() == [""] * 3
    assert table.column("altids").to_pylist() == [[]] * 3
    assert table.column("xhash").to_pylist() == [txhash.wide_bytes(0)] * 3
    assert [txhash.vhash_of(one) for one in table.column("hash").to_pylist()] == table.column(
        "vhash"
    ).to_pylist()

    # The header is lifted into columns of its own, still spelled exactly as
    # the payload spelled it: no number is read and no zone is named here.
    assert table.column("beginstring").to_pylist() == ["FIX.4.2", "FIX.4.4", None]
    assert table.column("bodylength").to_pylist() == ["176", None, None]
    assert table.column("msgseqnum").to_pylist() == ["7", "8", None]
    assert table.column("sendercompid").to_pylist() == ["BUY", "BUY", None]
    assert table.column("targetcompid").to_pylist() == ["XPAR", "XPAR", None]
    assert table.column("sendingtime").to_pylist() == [
        "20260814-09:30:00.123456789",
        None,
        None,
    ]

    # `SenderSubID <50>`, `OnBehalfOfCompID <115>` and `PossDupFlag <43>` are
    # header too, so they leave `entries` with the rest of it and answer from
    # columns of their own.
    assert [
        table.column(name).to_pylist()[0]
        for name in ("sendersubid", "onbehalfofcompid", "possdupflag")
    ] == ["DESK1", "CLIENTA", "Y"]
    first = table.column("entries")[0].as_py()
    assert [entry["key"] for entry in first] == [
        "11",
        "55",
        "54",
        "38",
        "44",
        "60",
        "58",
        "10",
    ], "entries keeps every token the standard header does not lift"
    assert first[-1] == {
        "tag": 10,
        "key": "10",
        "value": "203",
        "comp": None,
    }, "CheckSum is the boundary the lift is measured against, so it stays an entry"
    assert [entry["key"] for entry in table.column("entries")[1].as_py()] == [
        "555",
        "600",
        "55",
        "555",
        "55",
        "10",
    ], "a repeat the header does not name is nobody's business here"
    assert table.column("entries")[2].as_py() == []


def test_message_type_promotion_handles_wire_rendered_marked_and_repeated_keys(
    tmp_path: Path,
) -> None:
    log = _timed_log(
        tmp_path / "message-types.txt",
        ("2026-08-14 00:05:01.000", "35=D|Text=wire"),
        ("2026-08-14 00:05:02.000", "MsgType=8|Text=rendered"),
        ("2026-08-14 00:05:03.000", "#MSGTYPE=W|#Text=marked"),
        ("2026-08-14 00:05:04.000", "msg_type=G|Text=generic"),
        ("2026-08-14 00:05:05.000", "35=D|35=8|Text=first"),
    )
    log.msg_type_event_types = FixRegistry.from_builtin().msg_type_event_types()

    table = log.read_arrow_table()

    assert table.column("msgtype").to_pylist() == ["D", "8", "W", "G", "D"]
    assert table.column("eventtype").to_pylist() == [
        int(EventType.ORDER),
        int(EventType.EXECUTION),
        int(EventType.BOOK),
        int(EventType.ORDER),
        int(EventType.ORDER),
    ]
    keys = [[entry["key"] for entry in row] for row in table.column("entries").to_pylist()]
    assert keys == [
        ["Text"],
        ["Text"],
        ["Text"],
        ["Text"],
        # One spelling stating two values is torn, like every other header
        # field: both readings stay, and the column falls back to the raw
        # line's own first discriminator.
        ["35", "35", "Text"],
    ]


def test_the_standard_header_is_lifted_by_tag_before_the_checksum(tmp_path: Path) -> None:
    """The six fields beside `MsgType`, and the three ways a row keeps them down."""
    log = _timed_log(
        tmp_path / "session-header.txt",
        (
            "2026-08-14 00:05:01.000",
            "8=FIX.4.4|9=52|35=D|34=9|49=BUY|56=XPAR|52=20260814-09:30:00.000|11=A|10=001|",
        ),
        ("2026-08-14 00:05:02.000", "35=D|49=BUY|49=BUY|11=B|10=002|"),
        ("2026-08-14 00:05:03.000", "35=D|49=BUY|49=SELL|11=C|10=003|"),
        ("2026-08-14 00:05:04.000", "35=D|11=D|10=004|49=LATE|"),
        ("2026-08-14 00:05:05.000", "#MsgType=8|#SendingTime=20260814-09:30:00.000|#Text=x|"),
    )

    table = log.read_arrow_table()

    assert table.column("beginstring").to_pylist() == ["FIX.4.4", None, None, None, None]
    assert table.column("bodylength").to_pylist() == ["52", None, None, None, None]
    assert table.column("msgtype").to_pylist() == ["D", "D", "D", "D", "8"]
    assert table.column("msgseqnum").to_pylist() == ["9", None, None, None, None]
    assert table.column("targetcompid").to_pylist() == ["XPAR", None, None, None, None]
    assert table.column("sendercompid").to_pylist() == [
        "BUY",
        "BUY",  # spelled twice with one reading: still one statement of the fact
        None,  # spelled twice with two readings: neither is lifted
        None,  # spelled after the CheckSum, which is where eligibility ends
        None,
    ]
    assert table.column("sendingtime").to_pylist() == [
        "20260814-09:30:00.000",
        None,
        None,
        None,
        None,  # `SendingTime=` is a name, and only the tag `52` is read
    ]

    keys = [[entry["key"] for entry in row] for row in table.column("entries").to_pylist()]
    assert keys == [
        ["11", "10"],
        ["11", "10"],
        ["49", "49", "11", "10"],
        ["11", "10", "49"],
        ["SendingTime", "Text"],
    ], "a field that is not lifted stays where a reader can see every occurrence"


def test_text_file_has_no_protocol_codec_option(wire: Path) -> None:
    with pytest.raises(TypeError, match="codec"):
        TextFile.from_path(wire, codec=object())


def test_protocol_rules_are_caller_owned(wire: Path) -> None:
    assert TextFile.from_path(wire).protocol_rules is None


# -- arrow reader -----------------------------------------------------------


@pytest.mark.parametrize("fixture", ["plain", "gzipped", "zstandard"])
def test_reader_parses_every_record(request: pytest.FixtureRequest, fixture: str) -> None:
    path: Path = request.getfixturevalue(fixture)
    with TextFile(url=path.as_uri()) as log:
        table = log.into_arrow_reader().read_all()
    assert table.num_rows == EXPECTED_RECORDS
    assert table.schema == log.schema


def test_reader_returns_a_record_batch_reader(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        reader = log.into_arrow_reader()
        assert isinstance(reader, pyarrow.RecordBatchReader)
        assert reader.schema == log.schema
        reader.read_all()


def _timed_log(path: Path, *rows: tuple[str, str]) -> TextFile:
    path.write_text(
        "".join(f"{stamp} [t] [plugin] (INFO) {message}\n" for stamp, message in rows),
        encoding="utf-8",
    )
    return TextFile.from_path(path)


def test_reader_includes_any_regex_and_excludes_any_match_before_projection(
    tmp_path: Path,
) -> None:
    log = _timed_log(
        tmp_path / "messages.txt",
        ("2026-08-14 00:05:01.000", "lower"),
        ("2026-08-14 00:05:02.000", "UPPER"),
        ("2026-08-14 00:05:03.000", "lower secret"),
        ("2026-08-14 00:05:04.000", "Lower"),
    )

    table = log.read_arrow_table(
        include_regexes=(r"^lower", r"^UPPER$"), exclude_regexes=(r"secret", r"UPPER")
    )
    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["lower"]
    assert table.column("sourcerownum").to_pylist() == [1]

    projected = log.read_arrow_table(
        pyarrow.schema([("body", pyarrow.binary())]),
        include_regexes=(r"lower", r"UPPER"),
        exclude_regexes=(r"secret",),
    )
    assert projected.to_pydict() == {"body": [b"lower", b"UPPER"]}


def test_regexes_match_the_complete_folded_message(tmp_path: Path) -> None:
    path = tmp_path / "continuation.txt"
    path.write_text(
        "2026-08-14 00:05:01.000 [t] [plugin] (INFO) first\n"
        "\tat visible.Trace.call(Trace.java:1)\n"
        "2026-08-14 00:05:02.000 [t] [plugin] (INFO) hidden\n"
        "\tat hidden.Trace.call(Trace.java:2)\n"
        "2026-08-14 00:05:03.000 [t] [plugin] (INFO) last\n"
    )

    table = TextFile.from_path(path).read_arrow_table(
        include_regexes=(r"Trace\.java",), exclude_regexes=(r"hidden\.Trace",)
    )
    assert table.column("body").cast(pyarrow.string()).to_pylist() == [
        "first\n\tat visible.Trace.call(Trace.java:1)"
    ]
    assert table.column("sourcerownum").to_pylist() == [1]


def test_message_regexes_count_unicode_characters_not_utf8_bytes(tmp_path: Path) -> None:
    log = _timed_log(
        tmp_path / "unicode.txt",
        ("2026-08-14 00:05:01.000", "é"),
        ("2026-08-14 00:05:02.000", "😀"),
        ("2026-08-14 00:05:03.000", "ab"),
    )

    table = log.read_arrow_table(include_regexes=(r"^.$",))
    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["é", "😀"]


def _counting_splitter(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """How many rows reach the key/value splitter, one entry per call."""
    parsed: list[int] = []
    original = entries.parse_arrow

    def counted(messages):  # noqa: ANN001, ANN202 - observes the parser boundary
        parsed.append(len(messages))
        return original(messages)

    monkeypatch.setattr(entries, "parse_arrow", counted)
    return parsed


def test_message_and_time_filters_run_before_entry_parsing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = _timed_log(
        tmp_path / "filtered.txt",
        ("2026-08-14 00:05:01.000", "outside A=1"),
        ("2026-08-14 00:05:02.000", "hidden B=2"),
        ("2026-08-14 00:05:03.000", "kept C=3"),
        ("2026-08-14 00:05:04.000", "at-end D=4"),
    )
    parsed = _counting_splitter(monkeypatch)
    table = log.read_arrow_table(
        exclude_regexes=(r"^hidden",),
        start_unix=unix_of("2026-08-14 00:05:02.000"),
        end_unix=unix_of("2026-08-14 00:05:04.000"),
    )

    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["kept C=3"]
    assert parsed == [], "a single incidental assignment is not a structured message"


def test_msgtype_filters_run_before_entry_parsing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = _timed_log(
        tmp_path / "msgtypes.txt",
        ("2026-08-14 00:05:01.000", "8=FIX.4.4|35=0|58=" + "A=1|" * 1000),
        ("2026-08-14 00:05:02.000", "#MSGTYPE=1|#Text=" + "B=2|" * 1000),
        ("2026-08-14 00:05:03.000", "8=FIX.4.4|35=D|11=kept|"),
        ("2026-08-14 00:05:04.000", "plain diagnostic"),
    )
    parsed = _counting_splitter(monkeypatch)
    table = log.read_arrow_table(exclude_msgtypes=("0", "1"))

    assert table.column("msgtype").to_pylist() == ["D", None]
    assert parsed == [1]

    included = log.read_arrow_table(include_msgtypes=("0", "D"), exclude_msgtypes=("0",))
    assert included.column("msgtype").to_pylist() == ["D"]


def test_msgtype_filters_retain_administrative_messages_by_default(tmp_path: Path) -> None:
    log = _timed_log(
        tmp_path / "admin-msgtypes.txt",
        ("2026-08-14 00:05:01.000", "35=0|Text=heartbeat|"),
        ("2026-08-14 00:05:02.000", "MsgType=1|Text=test|"),
    )

    assert log.read_arrow_table().column("msgtype").to_pylist() == ["0", "1"]


def test_technical_plugins_bypass_entry_parsing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "plugins.txt"
    path.write_text(
        "2026-08-14 00:05:01.000 [t] [JoLoKiA] (INFO) "
        + "Metric=1|" * 1_000
        + "\n2026-08-14 00:05:02.000 [t] [Bridge] (INFO) 35=D|11=kept|\n",
        encoding="utf-8",
    )
    parsed = _counting_splitter(monkeypatch)

    table = TextFile.from_path(path).read_arrow_table(
        batch_row_size=1, technical_plugins=("jolokia",)
    )

    assert table.column("plugin").to_pylist() == [Plugin.from_str("Bridge").into_stored()]
    assert table.column("msgtype").to_pylist() == ["D"]
    assert parsed == [1], "the technical payload never reaches the entry splitter"


def test_time_filter_runs_before_payload_utf8_decoding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "invalid-before-window.txt"
    path.write_bytes(
        b"2026-08-14 00:05:01.000 [t] [plugin] (INFO) "
        + b"\xff" * (1 << 20)
        + b"\n2026-08-14 00:05:02.000 [t] [plugin] (INFO) kept\n"
    )
    original = text_file_module._utf8

    def reject_dirty(values):  # noqa: ANN001, ANN202 - observes the conversion boundary
        assert all(b"\xff" not in (value or b"") for value in values.to_pylist())
        return original(values)

    monkeypatch.setattr(text_file_module, "_utf8", reject_dirty)
    table = TextFile.from_path(path).read_arrow_table(start_unix=unix_of("2026-08-14 00:05:02.000"))

    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["kept"]


def test_regex_arguments_are_lists_and_can_filter_every_message(tmp_path: Path) -> None:
    log = _timed_log(tmp_path / "one.txt", ("2026-08-14 00:05:01.000", "hidden"))

    assert list(log.into_arrow_batches(exclude_regexes=(r"hidden",))) == []
    with pytest.raises(TypeError, match="include_regexes must be a sequence"):
        log.into_arrow_reader(include_regexes="hidden").read_all()  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="exclude_regexes must contain only regex strings"):
        log.into_arrow_reader(exclude_regexes=(1,)).read_all()  # type: ignore[arg-type]
    with pytest.raises(pyarrow.ArrowInvalid, match="Invalid regular expression"):
        log.into_arrow_batches(include_regexes=(r"[",))
    with pytest.raises(TypeError, match="include_msgtypes must be a sequence"):
        log.into_arrow_reader(include_msgtypes="D").read_all()  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="exclude_msgtypes must contain only MsgType strings"):
        log.into_arrow_reader(exclude_msgtypes=(1,)).read_all()  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="technical_plugins must be a sequence"):
        log.into_arrow_reader(technical_plugins="plugin").read_all()  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="technical_plugins must contain only plugin strings"):
        log.into_arrow_reader(technical_plugins=(1,)).read_all()  # type: ignore[arg-type]


def test_start_is_inclusive_and_end_is_exclusive(tmp_path: Path) -> None:
    log = _timed_log(
        tmp_path / "bounded.txt",
        ("2026-08-14 00:05:01.000", "before"),
        ("2026-08-14 00:05:02.000", "start"),
        ("2026-08-14 00:05:03.000", "inside"),
        ("2026-08-14 00:05:04.000", "end"),
    )

    table = log.read_arrow_table(
        start_unix=unix_of("2026-08-14 00:05:02.000"),
        end_unix=unix_of("2026-08-14 00:05:04.000"),
    )
    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["start", "inside"]
    assert (
        list(
            log.into_arrow_batches(
                start_unix=unix_of("2026-08-14 00:05:02.000"),
                end_unix=unix_of("2026-08-14 00:05:02.000"),
            )
        )
        == []
    )


def test_duration_batches_use_an_exact_explicit_start_and_skip_empty_windows(
    tmp_path: Path,
) -> None:
    log = _timed_log(
        tmp_path / "duration.txt",
        ("2026-08-14 00:05:01.250", "first"),
        ("2026-08-14 00:05:02.249", "same-window"),
        ("2026-08-14 00:05:02.250", "boundary"),
        ("2026-08-14 00:05:04.500", "after-gap"),
    )
    start = unix_of("2026-08-14 00:05:01.250")

    batches = list(log.into_arrow_batches(start_unix=start, duration_ns=SECOND))
    assert [batch.column("body").cast(pyarrow.string()).to_pylist() for batch in batches] == [
        ["first", "same-window"],
        ["boundary"],
        ["after-gap"],
    ]


def test_duration_without_a_start_uses_the_first_unix_truncated_to_duration(
    tmp_path: Path,
) -> None:
    log = _timed_log(
        tmp_path / "implicit-duration.txt",
        ("2026-08-14 00:05:01.900", "first"),
        ("2026-08-14 00:05:02.000", "boundary"),
        ("2026-08-14 00:05:02.999", "same-window"),
        ("2026-08-14 00:05:03.000", "next"),
    )

    batches = list(log.into_arrow_batches(duration_ns=SECOND))
    assert [batch.column("body").cast(pyarrow.string()).to_pylist() for batch in batches] == [
        ["first"],
        ["boundary", "same-window"],
        ["next"],
    ]


def test_duration_keeps_the_row_bound_and_rejects_invalid_windows(tmp_path: Path) -> None:
    log = _timed_log(
        tmp_path / "busy.txt",
        *((f"2026-08-14 00:05:01.{index:03}", str(index)) for index in range(5)),
    )

    batches = list(log.into_arrow_batches(batch_row_size=2, duration_ns=SECOND))
    assert [batch.num_rows for batch in batches] == [2, 2, 1]
    filtered = _timed_log(
        tmp_path / "filtered-busy.txt",
        ("2026-08-14 00:05:01.000", "keep-1"),
        ("2026-08-14 00:05:01.100", "drop"),
        ("2026-08-14 00:05:01.200", "keep-2"),
        ("2026-08-14 00:05:01.300", "keep-3"),
    )
    batches = list(
        filtered.into_arrow_batches(
            batch_row_size=2, exclude_regexes=(r"^drop$",), duration_ns=SECOND
        )
    )
    assert [batch.num_rows for batch in batches] == [2, 1]
    with pytest.raises(ValueError, match="duration_ns must be a positive integer"):
        log.into_arrow_batches(duration_ns=0)
    with pytest.raises(ValueError, match="batch_row_size must be a positive integer"):
        log.into_arrow_batches(batch_row_size=0)
    with pytest.raises(ValueError, match="read_byte_size must be a positive integer"):
        log.into_arrow_batches(read_byte_size=0)
    with pytest.raises(ValueError, match="read_byte_size must be a positive integer"):
        log.into_arrow_batches(read_byte_size=-1)
    with pytest.raises(ValueError, match="batch_byte_size must be a positive integer"):
        log.into_arrow_batches(batch_byte_size=0)
    with pytest.raises(ValueError, match="start_unix must be less than or equal"):
        log.into_arrow_batches(start_unix=2, end_unix=1)


def test_time_options_validate_int64_before_an_empty_source_is_read(tmp_path: Path) -> None:
    path = tmp_path / "empty.txt"
    path.touch()
    log = TextFile.from_path(path)

    with pytest.raises(ValueError, match="start_unix must fit"):
        log.into_arrow_batches(start_unix=1 << 63)
    with pytest.raises(ValueError, match="end_unix must fit"):
        log.into_arrow_batches(end_unix=-(1 << 63) - 1)
    with pytest.raises(ValueError, match="duration_ns must be a positive integer"):
        log.into_arrow_batches(duration_ns=1 << 63)


def test_duration_handles_an_int64_wide_distance_from_the_start(tmp_path: Path) -> None:
    log = _timed_log(
        tmp_path / "wide-duration.txt",
        ("2026-08-14 00:05:01.000", "first"),
        ("2026-08-14 00:05:02.000", "second"),
    )

    batches = list(log.into_arrow_batches(start_unix=-(1 << 63), duration_ns=SECOND))
    assert [batch.column("body").cast(pyarrow.string()).to_pylist() for batch in batches] == [
        ["first"],
        ["second"],
    ]


def test_duration_rejects_a_window_that_recurs_after_a_later_one(tmp_path: Path) -> None:
    log = _timed_log(
        tmp_path / "unordered.txt",
        ("2026-08-14 00:05:01.100", "first"),
        ("2026-08-14 00:05:02.100", "later"),
        ("2026-08-14 00:05:01.200", "recurred"),
    )
    batches = iter(log.into_arrow_batches(duration_ns=SECOND))

    assert next(batches).column("body").cast(pyarrow.string()).to_pylist() == ["first"]
    with pytest.raises(ValueError, match="duration window recurs"):
        next(batches)


def test_first_row(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
        url = log.url

    first = table.slice(0, 1).to_pylist()[0]
    assert first["sourceurl"] == url
    assert first["unix"] == FIRST_UNIX
    assert first["threadname"] == "250-e7256476:9effef3e6a:72505"
    assert first["plugin"] == Plugin.UNKNOWN.into_stored()
    assert first["body"].startswith(b"-> [5] {trade")


def test_the_hour_column_is_the_instant_truncated(plain: Path) -> None:
    """The compact partition clock must agree with the nanosecond instant."""
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
    assert table.num_rows
    for row in table.to_pylist():
        assert row["unixpartition"] == (row["unix"] - row["unix"] % HOUR) // SECOND
        assert row["unixpartition"] % (HOUR // SECOND) == 0
        assert 0 <= row["unix"] - row["unixpartition"] * SECOND < HOUR


def test_unix_is_total_nanos_since_epoch(plain: Path) -> None:
    moment = datetime.datetime(2026, 8, 14, 0, 5, 1, 147_250, tzinfo=datetime.UTC)
    expected = int(moment.timestamp()) * 1_000_000_000 + 147_250 * 1_000
    assert expected == FIRST_UNIX

    with TextFile(url=plain.as_uri()) as log:
        unix = log.into_arrow_table().column("unix").to_pylist()

    assert unix[0] == expected
    assert unix == sorted(unix), "the sample is chronological, so parsing must keep it so"


def test_url_column_identifies_the_source(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
        assert set(table.column("sourceurl").to_pylist()) == {log.url}


def test_source_rownum_counts_physical_lines_so_a_fold_shifts_nothing() -> None:
    """The number has to address the file: `sed -n '<n>p' <sourceurl>` is the row."""
    lines = SAMPLE_BYTES.split(b"\n")
    expected = [index for index, line in enumerate(lines, start=1) if HEADER_PATTERN.match(line)]
    assert len(expected) == EXPECTED_RECORDS


def test_source_rownum_points_back_at_the_line_that_was_parsed(tmp_path: Path) -> None:
    path = tmp_path / "folded.txt"
    path.write_text(
        "2026-08-14 00:05:01.147 [t] [d] (INFO) first\n"
        "\tat com.example.Wrapped.evaluate(Wrapped.java:1)\n"
        "\tat com.example.Wrapped.evaluate(Wrapped.java:2)\n"
        "2026-08-14 00:05:02.147 [t] [d] (INFO) second\n"
    )
    with TextFile.from_path(path) as log:
        table = log.read_arrow_table()
    assert table.column("sourcerownum").to_pylist() == [1, 4]
    assert set(table.column("sourceurl").to_pylist()) == {log.url}


def test_a_batch_boundary_keeps_every_rownum_with_its_own_row(tmp_path: Path) -> None:
    """The counter runs over the whole file, not over one batch of it."""
    path = tmp_path / "many.txt"
    path.write_text(
        "".join(f"2026-08-14 00:05:{i // 60:02d}.147 [t] [d] (INFO) {i}\n" for i in range(10))
    )
    with TextFile.from_path(path) as log:
        table = log.read_arrow_table(batch_row_size=3)
    assert table.column("sourcerownum").to_pylist() == list(range(1, 11))


def test_the_value_hash_is_per_line_and_a_signed_int64(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
    stored = table.column("hash").to_pylist()
    hashes = table.column("vhash").to_pylist()
    assert len(set(hashes)) == EXPECTED_RECORDS, "distinct lines hash distinctly"
    assert all(-(2**63) <= digest < 2**63 for digest in hashes)
    assert [txhash.vhash_of(one) for one in stored] == hashes
    assert table.column("xhash").to_pylist() == [txhash.wide_bytes(0)] * EXPECTED_RECORDS


def test_event_hash_is_stable_across_reads(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as first, TextFile(url=plain.as_uri()) as second:
        assert first.into_arrow_table().column("hash").to_pylist() == (
            second.into_arrow_table().column("hash").to_pylist()
        )


def test_level_is_stripped_from_the_message(plain: Path) -> None:
    """`level` is parsed by the regex but not a column: it must not leak."""
    with TextFile(url=plain.as_uri()) as log:
        messages = log.into_arrow_table().column("body").cast(pyarrow.string()).to_pylist()
    assert not any(message.startswith(("(DEBUG)", "(INFO)", "(WARNING)")) for message in messages)


def test_continuations_fold_into_the_previous_message(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        messages = log.into_arrow_table().column("body").cast(pyarrow.string()).to_pylist()

    (folded,) = [m for m in messages if "java.lang.IllegalStateException" in m]
    assert folded.startswith("Expression from CODE-0000058 raised while evaluating")
    assert folded.count("\n") == EXPECTED_CONTINUATIONS


def test_continuations_are_dropped_when_folding_is_off(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_reader(fold_continuations=False).read_all()
    assert table.num_rows == EXPECTED_RECORDS
    assert all(
        "\n" not in message for message in table.column("body").cast(pyarrow.string()).to_pylist()
    )


@pytest.mark.parametrize("batch_row_size", [1, 5, 23, EXPECTED_RECORDS, 10_000])
def test_batching_does_not_change_the_result(plain: Path, batch_row_size: int) -> None:
    """The rows *and their messages*: a folded continuation is a message, not a row.

    Counting alone passes over a continuation dropped at a batch boundary,
    because dropping one never changes how many rows there are.
    """
    with TextFile(url=plain.as_uri()) as log:
        batches = list(log.into_arrow_reader(batch_row_size=batch_row_size))
    with TextFile(url=plain.as_uri()) as log:
        whole = log.into_arrow_table(batch_row_size=EXPECTED_RECORDS * 2)
    assert sum(batch.num_rows for batch in batches) == EXPECTED_RECORDS
    assert max(batch.num_rows for batch in batches) <= batch_row_size
    messages = [
        message
        for batch in batches
        for message in batch.column("body").cast(pyarrow.string()).to_pylist()
    ]
    assert messages == whole.column("body").cast(pyarrow.string()).to_pylist()


def test_large_payloads_stop_a_batch_before_the_row_limit(tmp_path: Path) -> None:
    log = _timed_log(
        tmp_path / "wide.txt",
        *((f"2026-08-14 00:05:0{index}.000", "x" * 100) for index in range(4)),
    )

    batches = list(log.into_arrow_batches(batch_row_size=100, batch_byte_size=1))

    assert [batch.num_rows for batch in batches] == [1, 1, 1, 1]
    assert [batch.column("body").cast(pyarrow.string())[0].as_py() for batch in batches] == [
        "x" * 100
    ] * 4


def test_one_file_is_read_once_at_a_time(tmp_path: Path) -> None:
    """Two parses share one handle, and the second rewinds it under the first.

    Both readers then split every buffer between them, which lands mid-line
    and hands a spliced record over as data. The refusal is the answer.
    """
    path = tmp_path / "app.txt"
    path.write_text(
        "".join(f"2026-08-14 00:05:{n:02d}.000 [t] [M] (INFO) m{n}\n" for n in range(12)),
        encoding="utf-8",
    )

    with TextFile.from_path(path) as log:
        first = log.into_arrow_reader(batch_row_size=3, read_byte_size=64)
        assert first.read_next_batch().column("body").cast(pyarrow.string()).to_pylist() == [
            "m0",
            "m1",
            "m2",
        ]
        with pytest.raises(ValueError, match="is already being read"):
            log.into_arrow_reader(batch_row_size=3)
        with pytest.raises(ValueError, match="is already being read"):
            log.into_arrow_batches()
        # Rows the second reader would have rewound past are still there.
        assert first.read_next_batch().column("body").cast(pyarrow.string()).to_pylist() == [
            "m3",
            "m4",
            "m5",
        ]
        first.close()
        again = log.into_arrow_reader(batch_row_size=3)
        assert again.read_next_batch().column("body").cast(pyarrow.string()).to_pylist() == [
            "m0",
            "m1",
            "m2",
        ]
        again.close()


def test_a_carriage_return_inside_a_truncated_row_is_payload(tmp_path: Path) -> None:
    """It is half a terminator only where a line ended, and this one did not."""
    path = tmp_path / "cut.txt"
    header = "2026-08-14 00:05:00.000 [t] [M] (INFO) "
    path.write_bytes(f"{header}AAA\rBBBBBBBBBB\n".encode())

    with TextFile.from_path(path) as log:
        table = log.into_arrow_table(max_row_byte_size=len(header) + 4)

    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["AAA\r"]
    assert table.column("reason").to_pylist() == [
        "row truncated at max_row_byte_size; dropped bytes: 10"
    ]


def test_a_row_is_bounded_by_max_row_byte_size_and_says_what_it_dropped(
    tmp_path: Path,
) -> None:
    """A writer that never closes a line must not decide how much memory is held."""
    path = tmp_path / "runaway.txt"
    header = "2026-08-14 00:05:00.000 [t] [M] (INFO) "
    path.write_text(
        f"{header}{'x' * 10_000}\n{header}short\n",
        encoding="utf-8",
    )
    bound = len(header) + 100

    with TextFile.from_path(path) as log:
        table = log.into_arrow_table(max_row_byte_size=bound, read_byte_size=64)

    messages = table.column("body").cast(pyarrow.string()).to_pylist()
    assert messages == ["x" * 100, "short"]
    assert table.column("reason").to_pylist() == [
        "row truncated at max_row_byte_size; dropped bytes: 9900",
        None,
    ]
    assert table.column("sourcerownum").to_pylist() == [1, 2]


def test_folded_continuations_stop_at_the_same_bound(tmp_path: Path) -> None:
    """A stack trace is folded into its row, so it is bounded by the same rule."""
    path = tmp_path / "trace.txt"
    header = "2026-08-14 00:05:00.000 [t] [M] (INFO) "
    path.write_text(f"{header}head\n" + "y" * 40 + "\n" + "z" * 40 + "\n", encoding="utf-8")

    with TextFile.from_path(path) as log:
        table = log.into_arrow_table(max_row_byte_size=len(header) + 10)

    # "head", the newline the fold puts back, and the five bytes left of the
    # first continuation; the second one has no room at all.
    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["head\nyyyyy"]
    assert table.column("reason").to_pylist() == [
        "row truncated at max_row_byte_size; dropped bytes: 76"
    ]


def test_a_row_that_exactly_fills_the_bound_is_not_reported_as_truncated(
    tmp_path: Path,
) -> None:
    """The terminator is not content, so a line the bound fits precisely is whole."""
    header = "2026-08-14 00:05:00.000 [t] [M] (INFO) "
    bound = len(header) + 10
    for name, ending in (("lf.txt", "\n"), ("crlf.txt", "\r\n"), ("bare.txt", "")):
        path = tmp_path / name
        path.write_bytes(f"{header}{'x' * 10}{ending}".encode())
        with TextFile.from_path(path) as log:
            table = log.into_arrow_table(max_row_byte_size=bound)
        assert table.column("body").cast(pyarrow.string()).to_pylist() == ["x" * 10], name
        assert table.column("reason").to_pylist() == [None], name


def test_the_byte_bounds_are_validated_before_the_source_is_read(tmp_path: Path) -> None:
    """An int32 offset addresses a whole binary array, so it bounds both."""
    log = _timed_log(tmp_path / "messages.txt", ("2026-08-14 00:05:01.000", "row"))

    with pytest.raises(ValueError, match="max_row_byte_size must be a positive integer"):
        log.into_arrow_batches(max_row_byte_size=0)
    with pytest.raises(ValueError, match="max_row_byte_size must be at most"):
        log.into_arrow_batches(max_row_byte_size=1 << 31)
    with pytest.raises(ValueError, match="batch_byte_size must be at most"):
        log.into_arrow_batches(batch_byte_size=1 << 31)


def test_a_bound_below_the_header_is_refused_rather_than_read_as_no_rows(
    tmp_path: Path,
) -> None:
    """Every dropped byte is on a row's `reason` or it is refused."""
    log = _timed_log(
        tmp_path / "messages.txt",
        ("2026-08-14 00:05:01.000", "first"),
        ("2026-08-14 00:05:02.000", "second"),
    )

    with pytest.raises(ValueError, match="before the header pattern could match it"):
        log.into_arrow_table(max_row_byte_size=30)


def test_a_leading_fragment_that_fits_is_still_only_a_continuation(tmp_path: Path) -> None:
    """A rotated capture opens mid-record, and that fragment belongs to no row."""
    path = tmp_path / "rotated.txt"
    path.write_text(
        "fragment of a record the previous file ended in\n"
        "2026-08-14 00:05:02.000 [t] [M] (INFO) second\n",
        encoding="utf-8",
    )

    with TextFile.from_path(path) as log:
        table = log.into_arrow_table()

    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["second"]


def test_a_byte_order_mark_is_not_part_of_the_first_record(tmp_path: Path) -> None:
    """A .NET or Java writer opens the file with one, and it is encoding, not data."""
    path = tmp_path / "bom.txt"
    path.write_bytes(
        b"\xef\xbb\xbf"
        + b"2026-08-14 00:05:01.000 [t] [M] (INFO) first\n"
        + b"2026-08-14 00:05:02.000 [t] [M] (INFO) second\n"
    )

    with TextFile.from_path(path) as log:
        table = log.into_arrow_table()

    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["first", "second"]
    assert table.column("sourcerownum").to_pylist() == [1, 2]


def test_a_drained_chunk_ending_on_a_payload_return_is_counted_whole(
    tmp_path: Path,
) -> None:
    """A chunk that ran out of `read_byte_size` carries no terminator to strip."""
    path = tmp_path / "cut.txt"
    header = "2026-08-14 00:05:00.000 [t] [M] (INFO) "
    path.write_bytes(f"{header}KEEPabc\rdefg\n".encode())

    with TextFile.from_path(path) as log:
        table = log.into_arrow_table(max_row_byte_size=len(header) + 4, read_byte_size=4)

    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["KEEP"]
    assert table.column("reason").to_pylist() == [
        "row truncated at max_row_byte_size; dropped bytes: 8"
    ]


def test_one_long_compressed_line_streams_across_tiny_reads(tmp_path: Path) -> None:
    """A physical line is accumulated once, not recopied for every compressed read."""
    payload = "diagnostic " + "x" * (1 << 18)
    raw = f"2026-08-14 00:05:00.000 [t] [M] (INFO) {payload}\n".encode()
    path = tmp_path / "wide.txt.gz"
    path.write_bytes(gzip.compress(raw))

    with TextFile.from_path(path) as log:
        table = log.into_arrow_table(read_byte_size=31, batch_byte_size=4_096)

    assert table.num_rows == 1
    assert table.column("body").cast(pyarrow.string())[0].as_py() == payload


def test_a_continuation_on_the_batch_boundary_is_still_folded(tmp_path: Path) -> None:
    """The row a continuation belongs to must still be reachable when it arrives."""
    path = tmp_path / "boundary.txt"
    with path.open("wb") as out:
        for index in range(6):
            out.write(b"2026-08-14 00:05:%02d.000_000 [t] [M] (INFO) r%d\n" % (index, index))
            if index == 3:  # the last row of a four-row batch
                out.write(b"\tat com.example.A.b(A.java:1)\n")
    with TextFile(url=path.as_uri()) as log:
        table = log.into_arrow_table(batch_row_size=4)
    assert table.num_rows == 6
    assert (
        table.column("body").cast(pyarrow.string())[3].as_py()
        == "r3\n\tat com.example.A.b(A.java:1)"
    )


@pytest.mark.parametrize("read_byte_size", [1, 7, 64, 1 << 20])
def test_read_byte_size_does_not_change_the_result(plain: Path, read_byte_size: int) -> None:
    """A record split across two reads must still be parsed once, whole."""
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_reader(read_byte_size=read_byte_size).read_all()
    assert table.num_rows == EXPECTED_RECORDS
    assert table.column("unix").to_pylist()[-1] == max(table.column("unix").to_pylist())


def test_reader_is_lazy_until_pulled(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        reader = log.into_arrow_reader(batch_row_size=1)
        assert log.tell() == 0  # nothing scanned yet
        reader.read_next_batch()
        assert log.tell() > 0
        reader.close()


def test_owned_reader_exports_the_arrow_stream(plain: Path) -> None:
    with TextFile.from_path(plain) as log:
        reader = log.into_arrow_reader(batch_row_size=5)
        imported = pyarrow.RecordBatchReader.from_stream(reader)

        assert imported.read_all().num_rows == EXPECTED_RECORDS
        reader.close()


def test_custom_header_pattern(tmp_path: Path) -> None:
    """A caller's pattern must supply the same groups the schema is built from.

    A *different* pattern, over a differently shaped line -- the timestamp
    written the way `datetime.isoformat()` writes it, which is one character
    shorter than the bundled shape and therefore cannot be sliced at the
    bundled offsets.
    """
    pattern = re.compile(
        rb"^(?P<timestamp>\S+)\|(?P<threadname>[^|]*)\|(?P<plugin>[^|]*)\|(?P<body>.*)$",
        re.DOTALL,
    )
    path = tmp_path / "custom.txt"
    path.write_bytes(
        b"2026-08-14T00:05:01.167520|t1|Mod|first\n2026-08-14T00:05:02.000001|t2|Mod|second\n"
    )
    with TextFile(url=path.as_uri(), header_pattern=pattern) as log:
        table = log.into_arrow_table()
    assert table.column("body").cast(pyarrow.string()).to_pylist() == ["first", "second"]
    assert [(unix // 1000) % 1_000_000 for unix in table.column("unix").to_pylist()] == [
        167520,
        1,
    ]


@pytest.mark.parametrize(
    ("stamp", "microsecond"),
    [
        (b"2026-08-14 00:05:01.167_520", 167520),  # the bundled shape
        (b"2026-08-14 00:05:01,167,520", 167520),  # which the bundled regex also admits
        (b"2026-08-14T00:05:01.167520", 167520),  # what `datetime.isoformat()` writes
        (b"2026-08-14 00:05:01.167520123", 167520),  # nanoseconds, truncated
        (b"2026-08-14 00:05:01", 0),  # no fraction at all
    ],
)
def test_a_timestamp_is_read_not_sliced_at_another_width(stamp: bytes, microsecond: int) -> None:
    """Slicing at fixed offsets is only sound at the width the pattern pins.

    One character short, the same slices land on other digits and cast
    happily: `.167520` came back as `.167200`, silently.
    """
    assert _local_micros([stamp])[0].as_py().microsecond == microsecond


def test_a_store_that_cannot_append_says_so(tmp_path: Path) -> None:
    """S3 and GCS have no append, and a log is written by appending.

    The refusal comes from `pyarrow.fs` itself, so no network is involved --
    what is tested is that it arrives naming the two things that do work
    instead of as an `ArrowNotImplementedError` from three frames down.
    """
    import pyarrow.fs

    store = pyarrow.fs.S3FileSystem(endpoint_override="http://127.0.0.1:1", region="us-east-1")
    log = TextFile(url="bucket/app.txt", filesystem=store)
    with pytest.raises(NotImplementedError, match="cannot append"):
        log._append(b"a line\n")


def test_from_path_takes_the_zone_too(plain: Path) -> None:
    """The documented example: a local log is the one most likely to be local time."""
    naive = TextFile.from_path(plain).read_arrow_table()
    zoned = TextFile.from_path(plain, timezone="Europe/Paris").read_arrow_table()
    assert zoned.column("unix").to_pylist() != naive.column("unix").to_pylist()
    assert (
        zoned.column("body").cast(pyarrow.string()).to_pylist()
        == naive.column("body").cast(pyarrow.string()).to_pylist()
    )
    assert zoned.column("vhash").to_pylist() == naive.column("vhash").to_pylist()
    assert zoned.column("xhash").to_pylist() == naive.column("xhash").to_pylist()
    assert zoned.column("hash").to_pylist() != naive.column("hash").to_pylist()


def test_reading_the_same_log_twice_reads_it_twice(plain: Path) -> None:
    """A reader takes the stream, so the next one has to be given a new one."""
    log = TextFile(url=plain.as_uri())
    assert [log.read_arrow_table().num_rows for _ in range(3)] == [EXPECTED_RECORDS] * 3


def test_a_write_is_visible_to_the_next_read_of_the_same_object(tmp_path: Path) -> None:
    """Reopening, not seeking: a seek rewinds the decoded position, not the file."""
    source = tmp_path / "in.txt"
    source.write_bytes(b"2026-08-14 00:05:01.167_520 [t] [M] (INFO) one\n")
    grown = tmp_path / "out.txt"
    rows = TextFile.from_path(source).read_arrow_table()
    writer = TextFile.from_path(grown)
    writer.append_arrow(rows)
    assert writer.read_arrow_table().num_rows == 1
    writer.append_arrow(rows)
    assert writer.read_arrow_table().num_rows == 2


@pytest.mark.parametrize("zone", [None, "Europe/Paris", "America/New_York", "Asia/Tokyo"])
def test_a_write_renders_the_zone_it_read(tmp_path: Path, plain: Path, zone: str | None) -> None:
    """`unix` is an instant and a line is a wall clock: rendering as UTC shifts it.

    And shifts it again on the next round trip, since reading adds the offset
    back -- so this compares the columns, not just the row count.
    """
    rows = TextFile.from_url(plain.as_uri(), timezone=zone).read_arrow_table()
    written = tmp_path / "written.txt"
    TextFile.from_url(written.as_uri(), timezone=zone).append_arrow(rows)
    back = TextFile.from_url(written.as_uri(), timezone=zone).read_arrow_table()
    # The rendered line is the header regex read backwards rather than the
    # bytes that were parsed. What has to survive is what the columns say.
    for column in ("unix", "unixpartition", "body"):
        assert back.column(column).to_pylist() == rows.column(column).to_pylist(), column


def test_reader_on_a_closed_log_raises(plain: Path) -> None:
    log = TextFile(url=plain.as_uri())
    log.close()
    with pytest.raises(ValueError, match="closed file"):
        log.into_arrow_reader()


# -- stream surface ---------------------------------------------------------


def test_plain_log_is_seekable(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        assert log.seekable()
        assert log.read(5) == b"2026-"
        assert log.tell() == 5
        log.seek(0)
        assert log.read(5) == b"2026-"


def test_compressed_log_is_not_seekable(gzipped: Path) -> None:
    with TextFile(url=gzipped.as_uri()) as log:
        assert not log.seekable()


def test_readinto(plain: Path) -> None:
    buffer = bytearray(5)
    with TextFile(url=plain.as_uri()) as log:
        assert log.readinto(buffer) == 5
    assert bytes(buffer) == b"2026-"


def test_is_read_only(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        assert log.readable()
        assert not log.writable()
        with pytest.raises(OSError):
            log.write(b"nope")


# -- laziness and lifecycle -------------------------------------------------


def test_nothing_is_opened_until_first_read(plain: Path) -> None:
    log = TextFile(url=plain.as_uri())
    assert "_stream" not in log.__dict__
    log.read(1)
    assert "_stream" in log.__dict__
    log.close()


def test_text_file_owns_an_injected_arrow_fileio_without_serializing_it() -> None:
    fileio = ArrowFile(opened=pyarrow.BufferReader(SAMPLE_BYTES))
    log = TextFile(url="memory.txt", fileio=fileio)

    assert log.fileio is fileio
    assert "fileio" not in log.into_dict()
    assert log.read_arrow_table().num_rows == EXPECTED_RECORDS


def test_closing_a_partial_remote_compressed_reader_purges_only_raw_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = pyarrow.fs._MockFileSystem()
    store.create_dir("captures")
    payload = gzip.compress(SAMPLE_BYTES)
    with store.open_output_stream("captures/app.txt.gz", compression=None) as stream:
        stream.write(payload)
    original = ArrowFile.spill

    def into_test_cache(self, local=None, *, temporary=False):  # noqa: ANN001, ANN202
        return original(self, tmp_path / "spill", temporary=temporary)

    monkeypatch.setattr(ArrowFile, "spill", into_test_cache)
    log = TextFile(url="captures/app.txt.gz", filesystem=store, spill=True)
    reader = log.into_arrow_reader(batch_row_size=1)
    assert reader.read_next_batch().num_rows == 1
    active = log.__dict__["_active_fileio"]
    target = Path(active.location)
    assert active.temporary
    assert target.read_bytes() == payload, "the spill is compressed, never a decoded 30 GB copy"

    reader.close()
    assert not target.exists()
    log.close()


def test_stream_is_cached(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        assert log._stream is log._stream


def test_missing_file_only_fails_on_read(tmp_path: Path) -> None:
    log = TextFile(url=tmp_path.joinpath("absent.txt").as_uri())
    with pytest.raises(FileNotFoundError):
        log.read()


def test_close_does_not_open(tmp_path: Path) -> None:
    """Closing an unread log must not open anything -- __del__ takes this path."""
    log = TextFile(url=tmp_path.joinpath("absent.txt").as_uri())
    log.close()
    assert log.closed
    assert "_stream" not in log.__dict__


def test_close_is_idempotent(plain: Path) -> None:
    log = TextFile(url=plain.as_uri())
    log.read(1)
    log.close()
    log.close()
    assert log.closed


@pytest.mark.parametrize("operation", [lambda f: f.read(), lambda f: f.tell(), lambda f: f.seek(0)])
def test_use_after_close_raises(plain: Path, operation) -> None:
    log = TextFile(url=plain.as_uri())
    log.read(1)
    log.close()
    with pytest.raises(ValueError, match="closed file"):
        operation(log)


def test_repr_shows_url(plain: Path) -> None:
    log = TextFile(url=plain.as_uri())
    assert "app.txt" in repr(log)
    log.close()


def test_a_crlf_log_parses_identically(plain: Path, tmp_path: Path) -> None:
    """Windows-written captures must not leak carriage returns into messages."""
    crlf = tmp_path / "crlf.txt"
    crlf.write_bytes(SAMPLE_BYTES.replace(b"\n", b"\r\n"))
    with TextFile(url=plain.as_uri()) as a, TextFile(url=crlf.as_uri()) as b:
        left, right = a.into_arrow_table(), b.into_arrow_table()
    assert left.drop_columns(["sourceurl"]).equals(right.drop_columns(["sourceurl"]))


# -- timezone: the wall clock is local, the instant is not ----------------


#: The first record line's wall clock, read out of the fixture rather than
#: assumed -- then pinned, so a regex regression cannot move both sides.
FIRST_CLOCK = datetime.datetime.fromisoformat(
    RECORDS[0][:26].decode().replace("_", "").replace(",", ".")
)


def test_the_fixture_starts_where_the_assertions_below_say() -> None:
    assert FIRST_CLOCK == datetime.datetime(2026, 8, 14, 0, 5, 1, 147250)  # noqa: DTZ001


def test_without_a_timezone_the_clock_is_read_as_utc() -> None:
    with TextFile.from_url(SAMPLE.resolve().as_uri()) as log:
        batch = next(iter(log.into_arrow_batches()))
    instant = FIRST_CLOCK.replace(tzinfo=datetime.UTC)
    assert batch.column("unix")[0].as_py() == int(instant.timestamp() * 1_000_000) * 1_000


def test_a_timezone_shifts_the_instant_by_its_offset() -> None:
    """Same characters in the file, different moment in time."""
    naive, paris, york = (
        next(iter(TextFile.from_url(SAMPLE.resolve().as_uri(), timezone=zone).into_arrow_batches()))
        .column("unix")[0]
        .as_py()
        for zone in (None, "Europe/Paris", "America/New_York")
    )
    assert paris == naive - 2 * 3_600 * 1_000_000_000, "CEST is UTC+2 in August"
    assert york == naive + 4 * 3_600 * 1_000_000_000, "EDT is UTC-4 in August"


def test_the_hour_follows_the_instant_and_not_the_wall_clock() -> None:
    """The reverse of the day and time columns this replaced, and deliberately.

    A partition has to be a function of the column a reader filters on, and
    that column is `unix`. So two logs written in different zones at the same
    instant land in the same partition -- which is the only reading under which
    partitioning on it prunes anything.
    """
    hours = {}
    for zone in (None, "Europe/Paris", "Pacific/Auckland"):
        with TextFile.from_url(SAMPLE.resolve().as_uri(), timezone=zone) as log:
            batch = next(iter(log.into_arrow_batches()))
        hours[zone] = (
            batch.column("unix")[0].as_py(),
            batch.column("unixpartition")[0].as_py(),
        )
    assert len({unix for unix, _ in hours.values()}) == 3, "the instants differ by zone"
    for unix, unixpartition in hours.values():
        assert unixpartition == (unix - unix % HOUR) // SECOND, (
            "and the partition follows each of them"
        )


def test_a_repeated_hour_resolves_rather_than_raising() -> None:
    """A DST fall-back hour is the calendar's doing, not a broken log --
    pyarrow would raise by default, which would kill the parse once a year."""
    import pyarrow

    from rekep.text.text_file import _unix_nanos

    ambiguous = pyarrow.array(
        [datetime.datetime(2026, 10, 25, 2, 30)], type=pyarrow.timestamp("us")
    )
    assert _unix_nanos(ambiguous, "Europe/Paris")[0].as_py() is not None


def test_timezone_boundaries_keep_the_declared_earliest_and_latest_policy() -> None:
    """A fall-back chooses the first instant; a spring gap clamps to the first valid one."""
    import pyarrow

    from rekep.text.text_file import _unix_nanos

    local = pyarrow.array(
        [
            datetime.datetime(2026, 10, 25, 2, 30),
            datetime.datetime(2026, 3, 29, 2, 30),
        ],
        type=pyarrow.timestamp("us"),
    )
    expected = [
        datetime.datetime(2026, 10, 25, 0, 30, tzinfo=datetime.UTC),
        datetime.datetime(2026, 3, 29, 1, 0, tzinfo=datetime.UTC),
    ]
    assert _unix_nanos(local, "Europe/Paris").to_pylist() == [
        int(value.timestamp() * 1_000_000_000) for value in expected
    ]


def test_a_pre_epoch_timestamp_lands_in_the_hour_that_contains_it() -> None:
    """Arrow has no modulo and its integer divide rounds toward zero, so this
    is the branch a two-kernel truncation gets wrong: `-1` would come out in
    the hour *after* the one containing it."""
    before = pyarrow.array([-1, -HOUR - 1, 0, HOUR, 3 * HOUR + 5], type=pyarrow.int64())
    hour_seconds = HOUR // SECOND
    partition = unix_partition_arrow(before)
    assert partition.type == pyarrow.int32()
    assert partition.to_pylist() == [
        -hour_seconds,
        -2 * hour_seconds,
        0,
        hour_seconds,
        3 * hour_seconds,
    ]


@pytest.mark.parametrize(
    "unix",
    [(-2_147_482_800 * SECOND) - 1, 2_147_486_400 * SECOND],
    ids=("before-lower-hour", "at-upper-hour"),
)
def test_a_partition_outside_signed_int32_seconds_is_refused(unix: int) -> None:
    with pytest.raises(pyarrow.ArrowInvalid, match="not in range"):
        unix_partition_arrow(pyarrow.array([unix], type=pyarrow.int64()))


# -- static values ----------------------------------------------------------


def test_static_values_land_at_the_end_in_insertion_order(plain: Path) -> None:
    """After the data columns, so adding one moves nothing a reader selects."""
    log = TextFile.from_path(plain, static_values={"bridge": "bridge-1", "shard": 7})
    table = log.read_arrow_table()
    assert table.schema.names[-2:] == ["bridge", "shard"]
    assert table.schema.names[:-2] == Message.into_field().into_arrow_schema().names
    assert table.column("bridge").to_pylist() == ["bridge-1"] * table.num_rows
    assert table.column("shard").to_pylist() == [7] * table.num_rows


def test_nothing_names_the_source_but_the_caller(plain: Path) -> None:
    """No column is hardcoded: a capture says what it is, or says nothing."""
    assert TextFile.from_path(plain).read_arrow_table().schema.names == (
        Message.into_field().into_arrow_schema().names
    )


def test_a_static_value_infers_its_arrow_type(plain: Path) -> None:
    # Not `body`, which the raw contract already declares: a static column of that
    # name would be a second column of that name, and the schema could answer neither.
    log = TextFile.from_path(
        plain, static_values={"label": "a", "count": 2, "ratio": 0.5, "flag": True}
    )
    schema = log.schema
    assert schema.field("label").type == pyarrow.string()
    assert schema.field("count").type == pyarrow.int64()
    assert schema.field("ratio").type == pyarrow.float64()
    assert schema.field("flag").type == pyarrow.bool_()


def test_a_static_value_can_state_its_type(plain: Path) -> None:
    """A scalar is the explicit form -- and the only way to say a typed null."""
    log = TextFile.from_path(
        plain,
        static_values={
            "desk": pyarrow.scalar("EU", pyarrow.large_string()),
            "region": pyarrow.scalar(None, pyarrow.string()),
        },
    )
    assert log.schema.field("desk").type == pyarrow.large_string()
    assert log.schema.field("region").type == pyarrow.string()
    assert log.schema.field("region").nullable is True
    assert log.schema.field("desk").nullable is False
    table = log.read_arrow_table()
    assert table.column("region").to_pylist() == [None] * table.num_rows


def test_a_static_value_of_none_is_refused(plain: Path) -> None:
    """Arrow's `null` type is a column no store can widen later."""
    log = TextFile.from_path(plain, static_values={"region": None})
    with pytest.raises(ValueError, match="has no Arrow type"):
        log.read_arrow_table()


def test_a_static_value_that_is_already_a_column_is_refused_by_name(plain: Path) -> None:
    """A duplicate raw-message column is refused before Arrow sees it."""
    for taken in ("unix", "hash", "code", "sourceurl", "body", "msgtype", "sendingtime"):
        log = TextFile.from_path(plain, static_values={taken: "x"})
        with pytest.raises(ValueError, match=f"static value '{taken}' is already a column"):
            log.into_struct_field()


def test_static_columns_are_not_written_back_into_a_line(plain: Path, tmp_path: Path) -> None:
    """A line is what the header says; a constant column is not in it."""
    rows = TextFile.from_path(plain, static_values={"bridge": "bridge-1"}).read_arrow_table()
    out = TextFile.from_path(tmp_path / "copy.txt")
    out.append_arrow(rows)
    assert out.read_arrow_table().num_rows == rows.num_rows


# -- the dataset ------------------------------------------------------------


def test_a_text_file_is_a_dataset(plain: Path) -> None:
    log = TextFile.from_path(plain)
    assert isinstance(log, Dataset)
    assert log.exists
    assert log.into_struct_field() is Message.into_field()
    assert log.read_arrow_table().num_rows == EXPECTED_RECORDS


def test_a_missing_file_does_not_exist_yet(tmp_path: Path) -> None:
    assert not TextFile.from_path(tmp_path / "absent.txt").exists


def test_reading_casts_only_when_asked(plain: Path) -> None:
    log = TextFile.from_path(plain)
    assert log.read_arrow_reader().schema.equals(Message.into_field().into_arrow_schema())
    narrow = pyarrow.schema([("body", pyarrow.large_binary())])
    assert log.read_arrow_reader(narrow).schema.field("body").type == pyarrow.large_binary()


def test_a_write_renders_lines_that_parse_back(plain: Path, tmp_path: Path) -> None:
    """The renderer is the header regex read backwards; the proof is a round trip."""
    source = TextFile.from_path(plain).read_arrow_table()
    written = TextFile.from_path(tmp_path / "written.txt")
    written.append_arrow(source)

    again = TextFile.from_path(tmp_path / "written.txt").read_arrow_table()
    assert again.num_rows == source.num_rows
    for column in ("unix", "unixpartition", "threadname", "plugin", "body"):
        assert again.column(column).to_pylist() == source.column(column).to_pylist(), column


def test_a_write_creates_the_file(tmp_path: Path) -> None:
    target = tmp_path / "fresh.txt"
    log = TextFile.from_path(target)
    assert not log.exists
    log.append_arrow(TextFile.from_path(SAMPLE).read_arrow_table())
    assert log.exists and target.stat().st_size > 0


def test_writes_append_rather_than_replace(plain: Path, tmp_path: Path) -> None:
    rows = TextFile.from_path(plain).read_arrow_table()
    target = TextFile.from_path(tmp_path / "appended.txt")
    target.append_arrow(rows)
    target.append_arrow(rows)
    assert target.read_arrow_table().num_rows == 2 * rows.num_rows


def test_commit_row_size_writes_in_chunks(plain: Path, tmp_path: Path) -> None:
    rows = TextFile.from_path(plain).read_arrow_table()
    target = TextFile.from_path(tmp_path / "chunked.txt")
    target.append_arrow_reader(rows.to_reader(max_chunksize=5), commit_row_size=5)
    assert target.read_arrow_table().num_rows == rows.num_rows


def test_a_write_casts_a_nearly_right_batch(tmp_path: Path) -> None:
    batch = pyarrow.RecordBatch.from_pydict(
        {
            "unix": pyarrow.array([1_786_665_901_147_250_000], pyarrow.int64()),
            "body": ["hello"],
            "threadname": ["t"],
            "plugin": [Plugin.from_str("d").into_stored()],
            "noise": ["dropped"],
        }
    )
    target = TextFile.from_path(tmp_path / "cast.txt")
    target.append_arrow(batch)
    parsed = target.read_arrow_table()
    assert parsed.column("body").cast(pyarrow.string()).to_pylist() == ["hello"]
    assert parsed.column("plugin").to_pylist() == [Plugin.from_str("d").into_stored()]


def test_a_text_file_cannot_merge(tmp_path: Path) -> None:
    log = TextFile.from_path(tmp_path / "merge.txt")
    with pytest.raises(ValueError, match="cannot merge"):
        log.append_arrow(TextFile.from_path(SAMPLE).read_arrow_table(), merge_by=True)


def test_an_empty_write_leaves_an_empty_file(tmp_path: Path) -> None:
    log = TextFile.from_path(tmp_path / "empty.txt")
    log.append_arrow_reader(iter(()))
    assert log.exists
    assert log.read_arrow_table().num_rows == 0


def test_create_with_adopts_a_shape(tmp_path: Path) -> None:
    log = TextFile.from_path(tmp_path / "shaped.txt")
    narrow = Field.from_arrow_schema(pyarrow.schema([("body", pyarrow.binary())]))
    log.create_with(narrow)
    assert log.exists
    assert log.into_struct_field() is narrow
    assert log.read_arrow_table().column_names == ["body"]
