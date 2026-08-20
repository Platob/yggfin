"""Where declarations live, and what is loaded right now."""

import pathlib

import pytest

from rekep import config
from rekep.dag import Dag
from rekep.dataset import Dataset
from rekep.job import Passthrough


@pytest.fixture(autouse=True)
def empty_registry() -> None:
    config.clear()
    yield
    config.clear()


# -- the folder ------------------------------------------------------------


def test_an_explicit_root_wins(tmp_path: pathlib.Path) -> None:
    assert config.folder("datasets", tmp_path) == tmp_path


def test_a_checkouts_own_stacks_folder_beats_the_user_config(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A repository that declares its own pipelines should not be quietly
    overridden by whatever is in a home directory."""
    (tmp_path / "stacks" / "datasets").mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config, "STACKS_HOME", pathlib.Path("stacks"))
    assert config.folder("datasets") == pathlib.Path("stacks/datasets")


def test_without_one_it_is_the_user_config_home(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config, "STACKS_HOME", pathlib.Path("stacks"))
    monkeypatch.setattr(config, "CONFIG_HOME", tmp_path / ".config" / "rekep")
    assert config.folder("jobs") == tmp_path / ".config" / "rekep" / "jobs"


def test_loading_a_missing_folder_is_not_an_error(tmp_path: pathlib.Path) -> None:
    """Nothing declared is a state, not a failure."""
    assert Dataset.load_all(tmp_path / "nowhere") == []


def test_dumping_makes_the_folder(tmp_path: pathlib.Path) -> None:
    target = tmp_path / "fresh"
    written = Dataset(schema="rekep.models.Log", uri="ds:/trading/logs").dump(target)
    assert written == target / "logs.yaml"
    assert written.read_text()


# -- the registry ----------------------------------------------------------


def test_loading_registers_what_it_read(tmp_path: pathlib.Path) -> None:
    Dataset(schema="rekep.models.Log", uri="ds:/trading/logs").dump(tmp_path)
    config.clear()
    Dataset.load_all(tmp_path)
    assert config.lookup("ds:/trading/logs") is not None


def test_any_spelling_of_the_uri_finds_it(tmp_path: pathlib.Path) -> None:
    """One identity, so one entry rather than three misses."""
    Dataset(schema="rekep.models.Log", uri="ds:/trading/logs").dump(tmp_path)
    for spelling in ("ds:/trading/logs", "rekep:/datasets/trading/logs", "/datasets/trading/logs"):
        assert config.lookup(spelling) is not None, spelling
    assert config.lookup("trading/logs", service="datasets") is not None


def test_a_dataset_and_a_job_sharing_a_name_are_two_entries(tmp_path: pathlib.Path) -> None:
    config.register(Dataset(schema="rekep.models.Log", uri="ds:/trading/orders"))
    config.register(Passthrough(uri="job:/trading/orders"))
    assert len(config.REGISTRY) == 2
    assert config.lookup("ds:/trading/orders") is not config.lookup("job:/trading/orders")


def test_registered_filters_by_service(tmp_path: pathlib.Path) -> None:
    config.register(Dataset(schema="rekep.models.Log", uri="ds:/a/b"))
    config.register(Passthrough(uri="job:/a/b"))
    config.register(Dag(uri="dag:/a/b"))
    assert len(config.registered()) == 3
    assert len(config.registered("datasets")) == 1
    assert len(config.registered("jobs")) == 1
    assert len(config.registered("dags")) == 1


def test_load_finds_it_without_reading_the_folder_twice(tmp_path: pathlib.Path) -> None:
    Dataset(schema="rekep.models.Log", uri="ds:/trading/logs").dump(tmp_path)
    first = Dataset.load("ds:/trading/logs", tmp_path)
    assert Dataset.load("ds:/trading/logs", tmp_path) is first, "same object, from the registry"


def test_load_says_where_it_looked(tmp_path: pathlib.Path) -> None:
    with pytest.raises(KeyError, match="no dataset"):
        Dataset.load("ds:/nowhere/at/all", tmp_path)


def test_a_dumped_dataset_round_trips(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(
        schema="rekep.models.ParsedMessage",
        uri="ds:/warehouse/trading/messages",
        properties={"team": "platform"},
        protocols={"iceberg": {"branch": "dev"}},
    )
    dataset.dump(tmp_path)
    config.clear()
    (loaded,) = Dataset.load_all(tmp_path)
    assert loaded == dataset
    assert str(loaded.resource_uri()) == "ds:/warehouse/trading/messages#dev"
