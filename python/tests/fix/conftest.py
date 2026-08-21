"""The fixture pages, served in one place.

`test_registry.py` and `test_sqlite.py` both need a registry that fetches from
`fixtures/` instead of the network, and there is one rule for which file a URL
means -- so it lives here rather than in each of them.
"""

from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_page(url: str) -> str:
    """The fixture page `url` names; `OSError` where the site would 404.

    Only `4.4` has field pages, so every other version behaves like one the
    network cannot serve right now -- which is the path a registry walking
    versions has to take anyway.
    """
    if url.endswith("fix-dictionary.html"):
        name = "fix-dictionary.html"
    elif "/4.4/" in url:
        name = url.rsplit("/", 1)[-1]
    else:
        raise OSError(f"404 {url}")
    path = FIXTURES / name
    if not path.exists():
        raise OSError(f"404 {url}")
    return path.read_text()
