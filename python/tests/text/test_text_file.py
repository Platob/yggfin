import datetime
import gzip
import re
from pathlib import Path

import pyarrow
import pyarrow.fs
import pytest

from rekep import Dataset, Field, FixMessage
from rekep.fix import FixCodec, FixRegistry
from rekep.fix.columns import COLUMNS, COMMON, FLAT, KWARGS, QUOTE, SESSION, STAMPS
from rekep.market import MIC, Event
from rekep.market.event import HOUR
from rekep.text import HEADER_PATTERN, TextFile
from rekep.text.text_file import _local_micros
from rekep.times import unix_of

SAMPLE = Path(__file__).parent.parent / "data" / "app_sample.txt"
SAMPLE_BYTES = SAMPLE.read_bytes()
DICTIONARY = Path(__file__).resolve().parents[3] / "data" / "fix"

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


#: Two wire messages and a line that is not one. The sample above carries no
#: FIX, and every tag here is spelled as a number, so no name has to be looked
#: up to read one: what these three lines pin is the seam itself -- which tags
#: lift, which stay, and where the lifted ones land. A dictionary is still what
#: says a tag *may* lift, so these are read under `codec` like every other
#: parse below, and never under whatever `~/.config/fix` happens to hold.
#:
#: The second message is a multi-leg quote -- `55` names two legs and `555`
#: counts them twice -- which is the row that must keep everything it said.
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


@pytest.fixture(scope="module")
def codec() -> FixCodec:
    """The dictionary this repository publishes, which is the one a test may read.

    `FixCodec()` would default to the *user's* cache (`~/.config/fix`), so a
    test that took it would assert against whatever the machine running it had
    scraped before -- passing where that cache is warm and failing on a fresh
    checkout, which is every machine CI ever parses on. Every test below that
    expects a tag to reach its column names this dictionary instead.
    """
    return FixCodec(registry=FixRegistry(cache_dir=DICTIONARY, offline=True))


# -- header pattern ---------------------------------------------------------


def test_header_pattern_splits_a_row() -> None:
    match = HEADER_PATTERN.match(RECORDS[0])
    assert match is not None
    assert match["timestamp"] == b"2026-08-14 00:05:01.147_250"
    assert match["thread_name"] == b"250-e7256476:9effef3e6a:72505"
    assert match["plugin_code"] == b"OMSSales_Enrichment"
    assert match["level"] == b"DEBUG"
    assert match["message"].startswith(b"-> [5] {trade")


def test_header_pattern_tolerates_a_missing_level() -> None:
    (row,) = [r for r in RECORDS if HEADER_PATTERN.match(r)["level"] is None]
    assert HEADER_PATTERN.match(row)["message"] == b"no level printed by this driver"


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
    out.write_arrow(rows)
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
        assert found.group("message") == b"x"


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


# -- schema -----------------------------------------------------------------


#: What a parsed line adds to the `Event` envelope, in declaration order: the
#: line itself, the two ordered pair lists, then the FIX fields
#: flattened out of them. Written out whole rather than counted, because this
#: tail is what a reader selects by name and a column that was renamed, moved
#: or quietly dropped is invisible to a count.
LINE_COLUMNS = [
    "source_url",
    "source_rownum",
    "thread_name",
    "plugin_code",
    "message",
    "protocol_code",
    "unix_source",
    "protocol_version",
    "protocol_version_source",
    "msg_seq_num",
    "kwargs",
    "parties",
    "trd_reg_timestamps",
    "side_trd_reg_timestamps",
    "isincode",
    "begin_string",
    "body_length",
    "msg_type",
    "check_sum",
    "sender_comp_id",
    "sender_sub_id",
    "sender_location_id",
    "target_comp_id",
    "target_sub_id",
    "target_location_id",
    "on_behalf_of_comp_id",
    "on_behalf_of_sub_id",
    "on_behalf_of_location_id",
    "deliver_to_comp_id",
    "deliver_to_sub_id",
    "deliver_to_location_id",
    "last_msg_seq_num_processed",
    "poss_dup_flag",
    "poss_resend",
    "sending_time",
    "orig_sending_time",
    "on_behalf_of_sending_time",
    "appl_ver_id",
    "cstm_appl_ver_id",
    "appl_ext_id",
    "message_encoding",
    "xml_data_len",
    "xml_data",
    "secure_data_len",
    "secure_data",
    "signature_length",
    "signature",
    "symbol",
    "security_id",
    "security_id_source",
    "security_type",
    "cfi_code",
    "security_exchange",
    "currency",
    "account",
    "cl_ord_id",
    "orig_cl_ord_id",
    "order_id",
    "exec_id",
    "side",
    "ord_type",
    "time_in_force",
    "ord_status",
    "exec_type",
    "order_qty",
    "price",
    "vwap",
    "cum_qty",
    "leaves_qty",
    "last_px",
    "last_qty",
    "transact_time",
    "text",
    "quote_id",
    "quote_req_id",
    "quote_type",
    "quote_status",
    "quote_reject_reason",
    "quote_resp_type",
    "quote_cancel_type",
    "bid_px",
    "offer_px",
    "bid_size",
    "offer_size",
    "def_bid_size",
    "def_offer_size",
    "valid_until_time",
    "no_quote_sets",
    "no_quote_entries",
    "quote_set_id",
    "quote_entry_id",
]

EXPECTED_FLAT_COLUMNS = 77
EXPECTED_LINE_COLUMNS = 91
EXPECTED_LOG_COLUMNS = 109


def test_schema(plain: Path) -> None:
    schema = TextFile(url=plain.as_uri()).schema
    assert schema.names == FixMessage.into_field().into_arrow_schema().names
    assert len(schema.names) == EXPECTED_LOG_COLUMNS
    assert schema.names[:3] == ["unix", "unix_hour", "etype"], "the envelope leads"
    assert len(LINE_COLUMNS) == EXPECTED_LINE_COLUMNS
    assert schema.names[-len(LINE_COLUMNS) :] == LINE_COLUMNS
    assert schema.field("unix").type == pyarrow.int64()
    assert schema.field("unix_hour").type == pyarrow.int64()
    assert schema.field("hash").type == pyarrow.int64()
    assert schema.field("etype").type == pyarrow.int32()
    assert schema.field("message").type == pyarrow.string()
    assert schema.field("protocol_code").type == pyarrow.string()


def test_the_flat_columns_are_the_ones_the_column_layer_names() -> None:
    """`rekep.fix.columns` names the tags and the column each lands in, `FixMessage`
    declares the type, and the list pinned above is derived from neither -- so a
    field added on one side only is either a tag lifted out of `kwargs` into a
    column nothing declares, or a column no message can ever fill.

    Every promoted field retains its canonical FIX name in metadata.
    """
    assert FLAT == SESSION + COMMON + QUOTE, "session, common market fields, then quotes"
    assert [column for _, column in FLAT] == list(COLUMNS.values())
    assert len(COLUMNS) == EXPECTED_FLAT_COLUMNS
    added = [
        name for name in COLUMNS.values() if name not in {*Event.into_field().names, "msg_seq_num"}
    ]
    assert LINE_COLUMNS[-len(added) :] == added


def test_a_flat_field_is_a_column_of_its_own_type_and_not_text(plain: Path) -> None:
    """Lifted out of the pairs and decoded, so a reader filters on
    the value and not on its spelling: `9=176` is a number, `43=Y` is a boolean,
    and `38=1200` is a quantity. `CheckSum` is the one that stays text, because
    `010` read as `10` no longer verifies.
    """
    schema = TextFile(url=plain.as_uri()).schema
    declared = {
        "begin_string": pyarrow.string(),
        "body_length": pyarrow.int64(),
        "msg_type": pyarrow.string(),
        "msg_seq_num": pyarrow.int64(),
        "poss_dup_flag": pyarrow.bool_(),
        "secure_data": pyarrow.binary(),
        "order_qty": pyarrow.float64(),
        "check_sum": pyarrow.string(),
    }
    assert {name: schema.field(name).type for name in declared} == declared
    assert all(schema.field(column).nullable for _, column in FLAT)
    assert not schema.field("code").nullable and not schema.field("code").nullable


def test_an_instant_a_message_carries_is_a_microsecond_utc_timestamp(plain: Path) -> None:
    schema = TextFile(url=plain.as_uri()).schema
    stamps = sorted(COLUMNS[tag] for tag in STAMPS)
    assert stamps == [
        "on_behalf_of_sending_time",
        "orig_sending_time",
        "sending_time",
        "transact_time",
        "valid_until_time",
    ]
    for column in stamps:
        assert schema.field(column).type == pyarrow.timestamp("us", tz="UTC"), column


def _tagged(scalar: pyarrow.Scalar) -> list[tuple[int, str]]:
    """One row of `kwargs` as the `(tag, value)` pairs the dictionary resolved."""
    return [(entry["tag"], entry["value"]) for entry in scalar.as_py() or () if entry["tag"]]


def test_the_stored_fields_are_nullable_and_their_members_are_not(plain: Path) -> None:
    """A list preserves duplicate keys; null and empty keep distinct meanings."""
    schema = TextFile(url=plain.as_uri()).schema
    assert schema.field("kwargs").type == KWARGS
    assert schema.field("kwargs").nullable


# -- what a message fills ---------------------------------------------------


def test_exact_fix_columns_also_fill_the_generic_envelope(wire: Path, codec: FixCodec) -> None:
    """Snake columns keep FIX identity while generic identifiers are derived."""
    table = TextFile.from_path(wire, codec=codec).read_arrow_table()
    assert table.column("symbol").to_pylist() == ["TTF", None, None]
    assert table.column("msg_seq_num").to_pylist() == [7, 8, None]
    assert table.column("code").to_pylist() == ["ORD-1", "", ""]
    assert table.column("code").to_pylist() == ["ORD-1", "", ""]
    assert table.column("msg_type").to_pylist() == ["D", "AB", None]
    assert table.column("check_sum").to_pylist() == ["203", "011", None]
    assert table.column("mic").to_pylist() == [int(MIC.from_str("XPAR"))] * 2 + [None]
    assert table.column("reason").to_pylist() == ["ok", None, None]


def test_session_direction_selects_the_venue_side_when_both_ids_look_like_mics(
    tmp_path: Path, codec: FixCodec
) -> None:
    path = tmp_path / "direction.txt"
    path.write_text(
        "2026-08-14 00:05:01.147 [t] [d] sending 8=FIX.4.4|35=D|49=BUY1|56=XPAR|\n"
        "2026-08-14 00:05:02.147 [t] [d] received 8=FIX.4.4|35=8|49=XPAR|56=BUY1|\n"
    )
    rows = TextFile.from_path(path, codec=codec).read_arrow_table()
    assert rows.column("mic").to_pylist() == [int(MIC.from_str("XPAR"))] * 2


def test_explicit_market_ids_keep_precedence_and_wire_order(
    tmp_path: Path, codec: FixCodec
) -> None:
    path = tmp_path / "markets.txt"
    path.write_text(
        "2026-08-14 00:05:01.147 [t] [d] "
        "8=FIX.4.4|35=D|30=XAMS|100=XPAR|275=XEUR|1301=XNAS|30=XLON|10=1|\n"
        "2026-08-14 00:05:02.147 [t] [d] 8=FIX.4.4|35=D|100=XPAR|10=2|\n"
    )
    rows = TextFile.from_path(path, codec=codec).read_arrow_table()

    assert rows.column("mic").to_pylist() == [
        int(MIC.from_str("XAMS")),
        int(MIC.from_str("XPAR")),
    ]
    assert _tagged(rows.column("kwargs")[0]) == [
        (30, "XAMS"),
        (100, "XPAR"),
        (275, "XEUR"),
        (1301, "XNAS"),
        (30, "XLON"),
    ]


def test_generic_codes_follow_the_parsed_identifier_fallbacks(
    tmp_path: Path, codec: FixCodec
) -> None:
    path = tmp_path / "keys.txt"
    messages = [
        "8=FIX.4.4|35=8|37=VENUE|11=CURRENT|41=ORIGINAL|17=EXEC|"
        "453=1|448=BUYSIDE|447=D|452=1|10=1|",
        "8=FIX.4.4|35=G|11=CURRENT|41=ORIGINAL|17=EXEC|10=2|",
        "8=FIX.4.4|35=8|17=EXEC|10=3|",
        "8=FIX.4.4|35=X|55=TTF|10=4|",
        "8=FIX.4.4|35=d|37=   |11=<null>|48=SEC-1|55=|10=5|",
        "heartbeat",
    ]
    path.write_text(
        "".join(
            f"2026-08-14 00:05:0{index}.147 [t] [d] (INFO) {message}\n"
            for index, message in enumerate(messages)
        )
    )

    table = TextFile.from_path(path, codec=codec).read_arrow_table()

    assert table.column("code").to_pylist() == [
        "VENUE",
        "ORIGINAL",
        "EXEC",
        "TTF",
        "SEC-1",
        "",
    ], "`OrigClOrdID <41>` before `ClOrdID <11>`: a lifecycle survives its amendments"
    assert table.column("security_id").to_pylist()[4] == "SEC-1"
    assert table.column("xhash").to_pylist()[:5] == [
        FixMessage.hash_of(value) for value in ("VENUE", "ORIGINAL", "EXEC", "TTF", "SEC-1")
    ]
    assert table.column("parties")[0].as_py() == [
        {
            "party_id": "BUYSIDE",
            "party_id_source": "D",
            "party_role": 1,
            "buffer": None,
        }
    ]
    assert table.column("xhash")[-1].as_py() == table.column("hash")[-1].as_py()


def test_a_tag_that_repeats_in_a_line_stays_in_the_pair_list(wire: Path, codec: FixCodec) -> None:
    """Lifting the first `55` of a multi-leg quote would answer "the symbol"
    with whichever leg came first -- a wrong answer that looks like a right one.

    So that row keeps every pair of the tags that repeat, `555` included, and
    its `Symbol` stays null; the tags occurring once on the *same*
    line still lift, which is what makes this a per-tag rule and not a per-row
    one. The last line carries no message at all, so its pair list is null and not
    empty.
    """
    table = TextFile.from_path(wire, codec=codec).read_arrow_table()
    assert [_tagged(one) for one in table.column("kwargs")] == [
        [],
        [(555, "2"), (600, "TTF"), (55, "SPREAD"), (555, "2"), (55, "OTHER")],
        [],
    ]
    assert table.column("kwargs")[2].as_py() is None, "and that last row is null, not empty"
    assert table.column("symbol").to_pylist()[1] is None
    assert table.column("sender_comp_id").to_pylist() == ["BUY", "BUY", None], "49 lifted on both"


def test_a_lifted_field_arrives_decoded_and_not_as_the_text_the_wire_carried(
    wire: Path, codec: FixCodec
) -> None:
    """`43=Y` is a boolean and `44=41.25` is a number, so a reader filters on
    the value rather than on the spelling the wire happened to use. Which type
    each decodes to is `FixMessage`'s declaration, pinned against the published
    dictionary in `tests/text/test_log.py`.
    """
    table = TextFile.from_path(wire, codec=codec).read_arrow_table()
    assert table.column("poss_dup_flag").to_pylist() == [True, None, None]
    assert table.column("order_qty").to_pylist() == [1200.0, None, None]
    assert table.column("price").to_pylist() == [41.25, None, None]
    assert table.column("side").to_pylist() == ["1", None, None]
    assert table.column("text").to_pylist() == ["ok", None, None]


def test_a_stamp_a_message_carries_lands_on_the_instant_it_spells(
    wire: Path, codec: FixCodec
) -> None:
    """Nanosecond wire fractions truncate to the declared microsecond width."""
    sent = datetime.datetime(2026, 8, 14, 9, 30, 0, 123456, tzinfo=datetime.UTC)
    traded = datetime.datetime(2026, 8, 14, 9, 29, 59, 500_000, tzinfo=datetime.UTC)

    table = TextFile.from_path(wire, codec=codec).read_arrow_table()
    assert table.column("sending_time").to_pylist() == [sent, None, None]
    assert table.column("transact_time").to_pylist() == [traded, None, None]


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


def test_first_row(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
        url = log.url

    first = table.slice(0, 1).to_pylist()[0]
    assert first["source_url"] == url
    assert first["unix"] == FIRST_UNIX
    assert first["thread_name"] == "250-e7256476:9effef3e6a:72505"
    assert first["plugin_code"] == "OMSSales_Enrichment"
    assert first["message"].startswith("-> [5] {trade")


def test_the_hour_column_is_the_instant_truncated(plain: Path) -> None:
    """The denormalised partition column must agree with the nanosecond column."""
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
    assert table.num_rows
    for row in table.to_pylist():
        assert row["unix_hour"] == row["unix"] - row["unix"] % HOUR
        assert row["unix_hour"] % HOUR == 0
        assert 0 <= row["unix"] - row["unix_hour"] < HOUR


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
        assert set(table.column("source_url").to_pylist()) == {log.url}


def test_source_rownum_counts_physical_lines_so_a_fold_shifts_nothing() -> None:
    """The number has to address the file: `sed -n '<n>p' <source_url>` is the row."""
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
    assert table.column("source_rownum").to_pylist() == [1, 4]
    assert set(table.column("source_url").to_pylist()) == {log.url}


def test_a_batch_boundary_keeps_every_rownum_with_its_own_row(tmp_path: Path) -> None:
    """The counter runs over the whole file, not over one batch of it."""
    path = tmp_path / "many.txt"
    path.write_text(
        "".join(f"2026-08-14 00:05:{i // 60:02d}.147 [t] [d] (INFO) {i}\n" for i in range(10))
    )
    with TextFile.from_path(path) as log:
        table = log.read_arrow_table(batch_row_size=3)
    assert table.column("source_rownum").to_pylist() == list(range(1, 11))


def test_the_digest_is_per_line_and_a_signed_int64(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
    hashes = table.column("hash").to_pylist()
    assert len(set(hashes)) == EXPECTED_RECORDS, "distinct lines hash distinctly"
    assert all(-(2**63) <= digest < 2**63 for digest in hashes)
    assert table.column("xhash").to_pylist() == hashes, "a line is its own lifecycle"


def test_hash64_is_stable_across_reads(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as first, TextFile(url=plain.as_uri()) as second:
        assert first.into_arrow_table().column("hash").to_pylist() == (
            second.into_arrow_table().column("hash").to_pylist()
        )


def test_level_is_stripped_from_the_message(plain: Path) -> None:
    """`level` is parsed by the regex but not a column: it must not leak."""
    with TextFile(url=plain.as_uri()) as log:
        messages = log.into_arrow_table().column("message").to_pylist()
    assert not any(message.startswith(("(DEBUG)", "(INFO)", "(WARNING)")) for message in messages)


def test_continuations_fold_into_the_previous_message(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        messages = log.into_arrow_table().column("message").to_pylist()

    (folded,) = [m for m in messages if "java.lang.IllegalStateException" in m]
    assert folded.startswith("Expression from CODE-0000058 raised while evaluating")
    assert folded.count("\n") == EXPECTED_CONTINUATIONS


def test_continuations_are_dropped_when_folding_is_off(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_reader(fold_continuations=False).read_all()
    assert table.num_rows == EXPECTED_RECORDS
    assert all("\n" not in message for message in table.column("message").to_pylist())


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
    messages = [message for batch in batches for message in batch.column("message").to_pylist()]
    assert messages == whole.column("message").to_pylist()


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
    assert table.column("message")[3].as_py() == "r3\n\tat com.example.A.b(A.java:1)"


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


def test_custom_header_pattern(tmp_path: Path) -> None:
    """A caller's pattern must supply the same groups the schema is built from.

    A *different* pattern, over a differently shaped line -- the timestamp
    written the way `datetime.isoformat()` writes it, which is one character
    shorter than the bundled shape and therefore cannot be sliced at the
    bundled offsets.
    """
    pattern = re.compile(
        rb"^(?P<timestamp>\S+)\|(?P<thread_name>[^|]*)\|(?P<plugin_code>[^|]*)\|(?P<message>.*)$",
        re.DOTALL,
    )
    path = tmp_path / "custom.txt"
    path.write_bytes(
        b"2026-08-14T00:05:01.167520|t1|Mod|first\n2026-08-14T00:05:02.000001|t2|Mod|second\n"
    )
    with TextFile(url=path.as_uri(), header_pattern=pattern) as log:
        table = log.into_arrow_table()
    assert table.column("message").to_pylist() == ["first", "second"]
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
    assert zoned.column("message").to_pylist() == naive.column("message").to_pylist()
    assert zoned.column("hash").to_pylist() == naive.column("hash").to_pylist(), (
        "same lines, so the same digests -- the zone moves the instant, not the text"
    )


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
    writer.write_arrow(rows)
    assert writer.read_arrow_table().num_rows == 1
    writer.write_arrow(rows)
    assert writer.read_arrow_table().num_rows == 2


@pytest.mark.parametrize("zone", [None, "Europe/Paris", "America/New_York", "Asia/Tokyo"])
def test_a_write_renders_the_zone_it_read(tmp_path: Path, plain: Path, zone: str | None) -> None:
    """`unix` is an instant and a line is a wall clock: rendering as UTC shifts it.

    And shifts it again on the next round trip, since reading adds the offset
    back -- so this compares the columns, not just the row count.
    """
    rows = TextFile.from_url(plain.as_uri(), timezone=zone).read_arrow_table()
    written = tmp_path / "written.txt"
    TextFile.from_url(written.as_uri(), timezone=zone).write_arrow(rows)
    back = TextFile.from_url(written.as_uri(), timezone=zone).read_arrow_table()
    # Not `hash`: it is the digest of the *raw* line, and a rendered line is
    # the header regex read backwards, not the bytes that were parsed -- the
    # level marker a log prints is stripped into `message` and never rendered
    # back. What has to survive is what the columns say.
    for column in ("unix", "unix_hour", "message"):
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
    assert left.drop_columns("source_url").equals(right.drop_columns("source_url"))


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
        hours[zone] = (batch.column("unix")[0].as_py(), batch.column("unix_hour")[0].as_py())
    assert len({unix for unix, _ in hours.values()}) == 3, "the instants differ by zone"
    for unix, unix_hour in hours.values():
        assert unix_hour == unix - unix % HOUR, "and the hour follows each of them"


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
    import pyarrow

    from rekep.market.event import hour_arrow

    before = pyarrow.array([-1, -HOUR - 1, 0, HOUR, 3 * HOUR + 5], type=pyarrow.int64())
    assert hour_arrow(before).to_pylist() == [-HOUR, -2 * HOUR, 0, HOUR, 3 * HOUR]


# -- static values ----------------------------------------------------------


def test_static_values_land_at_the_end_in_insertion_order(plain: Path) -> None:
    """After the data columns, so adding one moves nothing a reader selects."""
    log = TextFile.from_path(plain, static_values={"bridge": "bridge-1", "shard": 7})
    table = log.read_arrow_table()
    assert table.schema.names[-2:] == ["bridge", "shard"]
    assert table.schema.names[:-2] == FixMessage.into_field().into_arrow_schema().names
    assert table.column("bridge").to_pylist() == ["bridge-1"] * table.num_rows
    assert table.column("shard").to_pylist() == [7] * table.num_rows


def test_nothing_names_the_source_but_the_caller(plain: Path) -> None:
    """No column is hardcoded: a capture says what it is, or says nothing."""
    assert TextFile.from_path(plain).read_arrow_table().schema.names == (
        FixMessage.into_field().into_arrow_schema().names
    )


def test_a_static_value_infers_its_arrow_type(plain: Path) -> None:
    # Not `text`, which `FixMessage` declares for `Text <58>`: a static column of that
    # name is a second column of that name, and the schema then answers neither.
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
    """A duplicate column reads as an absent one, which is the worst way to find out.

    The shape carries a column per lifted FIX field now, so `text`, `account`,
    `side` and `price` are all names a caller would plausibly reach for -- and
    appending a second `text` gave a schema with two of them, where the next
    `schema.field("text")` raised `KeyError: Column text does not exist`.
    """
    for taken in ("text", "account", "side", "price", "symbol", "message"):
        log = TextFile.from_path(plain, static_values={taken: "x"})
        with pytest.raises(ValueError, match=f"static value '{taken}' is already a column"):
            log.into_struct_field()
    # And a name the shape does not have is still perfectly fine.
    assert TextFile.from_path(plain, static_values={"desk": "x"}).into_struct_field().names[-1] == (
        "desk"
    )


def test_static_columns_are_not_written_back_into_a_line(plain: Path, tmp_path: Path) -> None:
    """A line is what the header says; a constant column is not in it."""
    rows = TextFile.from_path(plain, static_values={"bridge": "bridge-1"}).read_arrow_table()
    out = TextFile.from_path(tmp_path / "copy.txt")
    out.write_arrow(rows)
    assert out.read_arrow_table().num_rows == rows.num_rows


# -- the dataset ------------------------------------------------------------


def test_a_text_file_is_a_dataset(plain: Path) -> None:
    log = TextFile.from_path(plain)
    assert isinstance(log, Dataset)
    assert log.exists
    assert log.into_struct_field() is FixMessage.into_field()
    assert log.read_arrow_table().num_rows == EXPECTED_RECORDS


def test_a_missing_file_does_not_exist_yet(tmp_path: Path) -> None:
    assert not TextFile.from_path(tmp_path / "absent.txt").exists


def test_reading_casts_only_when_asked(plain: Path) -> None:
    log = TextFile.from_path(plain)
    assert log.read_arrow_reader().schema.equals(FixMessage.into_field().into_arrow_schema())
    narrow = pyarrow.schema([("message", pyarrow.large_string())])
    assert log.read_arrow_reader(narrow).schema.field("message").type == pyarrow.large_string()


def test_a_write_renders_lines_that_parse_back(plain: Path, tmp_path: Path) -> None:
    """The renderer is the header regex read backwards; the proof is a round trip."""
    source = TextFile.from_path(plain).read_arrow_table()
    written = TextFile.from_path(tmp_path / "written.txt")
    written.write_arrow(source)

    again = TextFile.from_path(tmp_path / "written.txt").read_arrow_table()
    assert again.num_rows == source.num_rows
    for column in ("unix", "unix_hour", "thread_name", "plugin_code", "message"):
        assert again.column(column).to_pylist() == source.column(column).to_pylist(), column


def test_a_write_creates_the_file(tmp_path: Path) -> None:
    target = tmp_path / "fresh.txt"
    log = TextFile.from_path(target)
    assert not log.exists
    log.write_arrow(TextFile.from_path(SAMPLE).read_arrow_table())
    assert log.exists and target.stat().st_size > 0


def test_writes_append_rather_than_replace(plain: Path, tmp_path: Path) -> None:
    rows = TextFile.from_path(plain).read_arrow_table()
    target = TextFile.from_path(tmp_path / "appended.txt")
    target.write_arrow(rows)
    target.write_arrow(rows)
    assert target.read_arrow_table().num_rows == 2 * rows.num_rows


def test_commit_row_size_writes_in_chunks(plain: Path, tmp_path: Path) -> None:
    rows = TextFile.from_path(plain).read_arrow_table()
    target = TextFile.from_path(tmp_path / "chunked.txt")
    target.write_arrow_reader(rows.to_reader(max_chunksize=5), commit_row_size=5)
    assert target.read_arrow_table().num_rows == rows.num_rows


def test_a_write_casts_a_nearly_right_batch(tmp_path: Path) -> None:
    batch = pyarrow.RecordBatch.from_pydict(
        {
            "unix": pyarrow.array([1_786_665_901_147_250_000], pyarrow.int64()),
            "message": ["hello"],
            "thread_name": ["t"],
            "plugin_code": ["d"],
            "noise": ["dropped"],
        }
    )
    target = TextFile.from_path(tmp_path / "cast.txt")
    target.write_arrow(batch)
    parsed = target.read_arrow_table()
    assert parsed.column("message").to_pylist() == ["hello"]
    assert parsed.column("plugin_code").to_pylist() == ["d"]


def test_a_text_file_cannot_merge(tmp_path: Path) -> None:
    log = TextFile.from_path(tmp_path / "merge.txt")
    with pytest.raises(ValueError, match="cannot merge"):
        log.write_arrow(TextFile.from_path(SAMPLE).read_arrow_table(), merge_by=True)


def test_an_empty_write_leaves_an_empty_file(tmp_path: Path) -> None:
    log = TextFile.from_path(tmp_path / "empty.txt")
    log.write_arrow_reader(iter(()))
    assert log.exists
    assert log.read_arrow_table().num_rows == 0


def test_create_with_adopts_a_shape(tmp_path: Path) -> None:
    log = TextFile.from_path(tmp_path / "shaped.txt")
    narrow = Field.from_arrow_schema(pyarrow.schema([("message", pyarrow.string())]))
    log.create_with(narrow)
    assert log.exists
    assert log.into_struct_field() is narrow
    assert log.read_arrow_table().column_names == ["message"]
