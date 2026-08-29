"""The fixture pages, served in one place.

The registry tests need doubles that fetch from `fixtures/` instead of the
network, and there is one rule for which file a URL means -- so it lives here
rather than being copied into each of them.
"""

from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_page(url: str) -> str:
    """The fixture page `url` names; `OSError` where the site would 404.

    Only `4.4` has field pages, so every other version behaves like one the
    network cannot serve right now -- which is the path a registry walking
    versions has to take anyway. QuickFIX spec files are served by their own
    file name, so a parser test never reaches GitHub.
    """
    if "nanoconda.com/fix-reference/" in url:
        relative = url.split("nanoconda.com/fix-reference/", 1)[1]
        path = FIXTURES / "nanoconda" / relative
        if path.exists():
            return path.read_text()
        raise OSError(f"404 {url}")
    if url.endswith("fix-dictionary.html"):
        name = "fix-dictionary.html"
    elif url.endswith(".xml"):
        name = url.rsplit("/", 1)[-1]
    elif "/4.4/" in url:
        name = url.rsplit("/", 1)[-1]
    else:
        raise OSError(f"404 {url}")
    path = FIXTURES / name
    if not path.exists():
        raise OSError(f"404 {url}")
    return path.read_text()
