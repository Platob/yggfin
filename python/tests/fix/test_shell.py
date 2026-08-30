"""`rekep fix shell`: what each verb does, and what it refuses to write.

Driven through `reader`, which is where a prompt's answers come from -- so
every branch is reachable without a terminal, and the assertions are about the
store the session left behind rather than about the escapes it printed.
"""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from pathlib import Path

import pytest

from rekep.console import Console
from rekep.fields import Field
from rekep.fields.metadata import values_of
from rekep.fix.entries import ComponentRecord, record_kind
from rekep.fix.fields import fix_field, namespaced_field
from rekep.fix.quickfix import block, field_member, group_member
from rekep.fix.registry import FixRegistry
from rekep.fix.shell import Shell, terminal_reader


class Offline(FixRegistry):
    """A registry that must answer from the store alone."""

    def _fetch(self, url: str) -> str:
        raise OSError(f"offline: {url}")


def _field(name: str, tag: int, version: str, datatype: str = "String") -> Field:
    return fix_field(name, tag, datatype, version=version)


@pytest.fixture
def store(tmp_path: Path) -> Offline:
    """A store holding two synthetic fields and one synthetic component."""
    registry = Offline(cache_dir=tmp_path / "fix")
    registry._store_versions(("9.1",))
    registry._store_fields(
        "9.1",
        [_field("FakeRole", 90001, "9.1", "int"), _field("FakeCode", 90002, "9.1")],
        components=[
            block(
                "FakeParties",
                [
                    group_member(
                        "NoFakeParties", 90003, [field_member("FakeRole", 90001, required=True)]
                    )
                ],
            )
        ],
    )
    return registry


def _session(store: FixRegistry, *answers: str) -> tuple[Shell, io.StringIO]:
    """One shell whose answers are `answers`, and where its output went."""
    written = io.StringIO()
    replies = iter(answers)

    def reader(prompt: str) -> str:
        """What `input` does when there is nothing left: raise, not return."""
        del prompt
        try:
            return next(replies)
        except StopIteration:
            raise EOFError from None

    return (
        Shell(registry=store, console=Console(stream=written, colour=False), reader=reader),
        written,
    )


def _run(store: FixRegistry, *answers: str) -> str:
    shell, written = _session(store, *answers)
    assert shell.run() == 0
    return written.getvalue()


# -- reading it --------------------------------------------------------------


def test_a_session_ends_on_quit_and_on_the_end_of_input(store: Offline) -> None:
    assert "bye" in _run(store, "quit")
    shell, _ = _session(store)
    assert shell.run() == 0, "and on nothing at all, rather than raising"


def test_the_live_prompt_is_presentation_on_stderr(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.setattr("builtins.input", lambda: "show 35")
    assert terminal_reader("FIX > ") == "show 35"
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "FIX > "


def test_help_lists_every_verb_the_loop_dispatches(store: Offline) -> None:
    """So a verb added without a line of help is visible as a gap."""
    printed = _run(store, "help", "quit")
    verbs = {verb.split()[0] for verb, _ in Shell.into_help()}
    dispatched = set(Shell.into_commands()) - {"exit"}
    assert verbs == dispatched
    for verb in verbs:
        assert verb in printed


def test_help_is_grouped_and_can_explain_one_command(store: Offline) -> None:
    printed = _run(store, "help", "help show", "quit")
    assert all(title in printed for title in ("browse", "edit fields", "edit components", "store"))
    assert "show <name|tag>" in printed


def test_versions_says_what_the_store_holds(store: Offline) -> None:
    printed = _run(store, "versions", "quit")
    assert "9.1" in printed and "2" in printed


def test_find_searches_by_name_and_by_tag(store: Offline) -> None:
    assert "FakeRole" in _run(store, "find FakeRole", "quit")
    assert "FakeCode" in _run(store, "find 90002", "quit")


def test_find_shows_one_row_per_identity_across_versions(store: Offline) -> None:
    store._store_versions(("9.1", "9.2"))
    store._store_fields("9.2", [_field("FakeRole", 90001, "9.2")])
    printed = _run(store, "find FakeRole", "quit")
    rows = [line for line in printed.splitlines() if "FakeRole" in line and "searching" not in line]
    assert len(rows) == 1


def test_show_prints_one_identity_and_every_version_of_it(store: Offline) -> None:
    printed = _run(store, "show FakeRole", "quit")
    assert "FakeRole" in printed and "90001" in printed and "9.1" in printed


def test_show_accepts_a_numeric_tag(store: Offline) -> None:
    assert "FakeRole" in _run(store, "show 90001", "quit")


def test_large_field_and_component_details_are_bounded() -> None:
    registry = FixRegistry.from_builtin()
    field = _run(registry, "show SecurityType", "quit")
    component = _run(registry, "component Instrument", "quit")
    assert "more; `rekep fix registry show`" in field
    assert "more; `rekep fix registry component`" in component


def test_a_name_nothing_resolves_says_what_it_could_have_meant(store: Offline) -> None:
    printed = _run(store, "show FakeRolle", "quit")
    assert "no field 'FakeRolle'" in printed
    assert "did you mean" in printed and "FakeRole" in printed


def test_an_unknown_verb_is_reported_rather_than_ignored(store: Offline) -> None:
    assert "no command 'wat'" in _run(store, "wat", "quit")
    assert "did you mean `show`" in _run(store, "shwo FakeRole", "quit")


def test_components_and_component_read_the_declaration(store: Offline) -> None:
    listed = _run(store, "components", "quit")
    assert "FakeParties" in listed
    assert "msgtype" in listed, "a message is listed here too, and the code says which"
    printed = _run(store, "component FakeParties", "quit")
    assert "NoFakeParties" in printed
    assert "required" in printed, "the spec's own rule, which is what nullability reads"


def test_component_declarations_are_added_updated_and_removed(
    store: Offline, tmp_path: Path
) -> None:
    declaration = tmp_path / "legs.json"
    ComponentRecord(
        name="FakeLegs",
        versions=("9.1",),
        declaration=block("FakeLegs", [field_member("FakeCode", 90002)]),
    ).into_json(str(declaration))
    assert "added FakeLegs" in _run(store, f"add-component {declaration}", "y", "quit")
    assert store.merged_component("FakeLegs").members[0].name == "FakeCode"

    assert "updated FakeLegs" in _run(store, f"update-component {declaration}", "y", "quit")
    assert "kept" in _run(store, "remove-component FakeLegs", "n", "quit")
    assert "removed FakeLegs" in _run(store, "remove-component FakeLegs", "y", "quit")
    with pytest.raises(KeyError, match="FakeLegs"):
        store.merged_component("FakeLegs")


def test_complete_field_declarations_are_added_and_updated(store: Offline, tmp_path: Path) -> None:
    declaration = tmp_path / "venue.json"
    record = namespaced_field("FAKE.VENUE.CODE", "String")
    record.fix.enumerated = {"A": "Alpha"}
    declaration.write_text(json.dumps(record.into_dict()))
    assert "added FAKE.VENUE.CODE" in _run(store, f"add-field {declaration}", "y", "quit")
    assert store.resolve("FAKE.VENUE.CODE").fix.enumerated == values_of({"A": "Alpha"})

    assert "updated FAKE.VENUE.CODE" in _run(store, f"update-field {declaration}", "y", "quit")


# -- changing it -------------------------------------------------------------


def test_add_builds_one_identity_from_answered_questions(store: Offline) -> None:
    printed = _run(
        store,
        "add",
        "FakeVenue",  # name
        "90004",  # tag
        "9.1",  # version
        "String",  # type
        "A venue of ours.",  # description
        "fakevenue",  # column
        "y",  # write it
        "quit",
    )
    assert "added FakeVenue" in printed
    entry = store.resolve("FakeVenue")
    assert (entry.fix.tag, entry.fix.column) == (90004, "fakevenue")
    assert entry.description == "A venue of ours." and entry.fix.versions == ("9.1",)


def test_a_field_fix_never_numbered_is_added_by_leaving_the_tag_blank(store: Offline) -> None:
    _run(store, "add", "TECH.CLIENTID", "", "*", "String", "", "techclientid", "y", "quit")
    entry = store.resolve("TECH.CLIENTID")
    assert entry.fix.tag is None and record_kind(entry) == "namespace"


def test_nothing_is_written_until_the_whole_entry_has_been_shown_back(store: Offline) -> None:
    printed = _run(store, "add", "FakeVenue", "90004", "9.1", "String", "", "", "n", "quit")
    assert "nothing was written" in printed
    assert store.resolve("FakeVenue") is None


def test_edit_keeps_every_part_left_unanswered(store: Offline) -> None:
    """A bare Enter is "as it was", which is what makes editing one field one answer."""
    _run(store, "edit FakeRole", "", "", "", "", "", "renamedcolumn", "y", "quit")
    entry = store.resolve("FakeRole")
    assert (entry.fix.canonical, entry.fix.tag, entry.fix.column) == (
        "FakeRole",
        90001,
        "renamedcolumn",
    )
    assert entry.fix.type == "int", "and the type it already had"


def test_a_tag_that_is_not_a_number_is_refused_before_anything_is_written(
    store: Offline,
) -> None:
    printed = _run(store, "add", "FakeVenue", "nine", "9.1", "String", "", "", "quit")
    assert "'nine' is not a tag" in printed
    assert store.resolve("FakeVenue") is None


def test_a_duplicate_tag_is_refused_with_the_reason(store: Offline) -> None:
    """The registry's own check, reported here rather than raised as a traceback."""
    printed = _run(store, "add", "FakeOther", "90001", "9.1", "String", "", "", "y", "quit")
    assert "tag 90001 is already claimed by 'FakeRole'" in printed
    assert store.resolve("FakeOther") is None


def test_alias_records_a_spelling_with_where_it_was_counted(store: Offline) -> None:
    printed = _run(store, "alias FakeRole", "FAKEROLLE", "brk", "41", "y", "quit")
    assert "answers to" in printed
    (alias,) = store.resolve("FakeRole").fix.named_aliases
    assert (alias.name, alias.source, alias.occurrences) == ("FAKEROLLE", "brk", 41)


def test_alias_previews_and_refuses_invalid_or_unconfirmed_counts(store: Offline) -> None:
    printed = _run(store, "alias FakeRole", "FAKEROLLE", "brk", "41", "n", "quit")
    assert "nothing was written" in printed
    assert store.resolve("FakeRole").fix.named_aliases == ()

    printed = _run(store, "alias FakeRole", "FAKEROLLE", "brk", "-1", "quit")
    assert "non-negative whole number" in printed
    assert store.resolve("FakeRole").fix.named_aliases == ()


def test_remove_asks_first_and_keeps_it_when_the_answer_is_no(store: Offline) -> None:
    assert "kept" in _run(store, "remove FakeCode", "n", "quit")
    assert store.resolve("FakeCode") is not None
    assert "removed FakeCode" in _run(store, "remove FakeCode", "y", "quit")
    assert store.resolve("FakeCode") is None


def test_check_reports_a_sound_store_as_sound(store: Offline) -> None:
    assert "this store is sound" in _run(store, "check", "quit")


def test_dump_writes_the_store_where_it_is_told(store: Offline, tmp_path: Path) -> None:
    target = tmp_path / "folder with spaces" / "dumped.zip"
    target.parent.mkdir()
    assert "nothing was written" in _run(store, f'dump "{target}"', "n", "quit")
    assert not target.exists()
    assert "wrote" in _run(store, f'dump "{target}"', "y", "quit")
    assert target.exists()
    assert FixRegistry(cache_dir=target).field("FakeRole", "9.1").name == "FakeRole"


def test_load_opens_another_store_in_the_same_session(store: Offline, tmp_path: Path) -> None:
    target = store.into_zip(tmp_path / "other.zip")
    printed = _run(store, f"load {target}", "versions", "quit")
    assert "1 versions" in printed


def test_a_verb_that_needs_an_argument_says_so_rather_than_failing(store: Offline) -> None:
    for line in ("find", "show", "component", "load", "dump"):
        assert "say " in _run(store, line, "quit") or "name " in _run(store, line, "quit")


def test_an_interrupt_mid_question_cancels_without_writing(store: Offline) -> None:
    """Ctrl-C halfway through building a field must not leave half of one behind."""
    replies: Callable[[str], str] = _interrupting(["add", "FakeVenue"])
    written = io.StringIO()
    shell = Shell(registry=store, console=Console(stream=written, colour=False), reader=replies)
    assert shell.run() == 0
    assert "cancelled" in written.getvalue()
    assert store.resolve("FakeVenue") is None


def _interrupting(answers: list[str]) -> Callable[[str], str]:
    """A reader that hands back `answers` and then interrupts, once, then ends."""
    remaining = list(answers)
    state = {"interrupted": False}

    def reader(prompt: str) -> str:
        del prompt
        if remaining:
            return remaining.pop(0)
        if not state["interrupted"]:
            state["interrupted"] = True
            raise KeyboardInterrupt
        raise EOFError

    return reader
