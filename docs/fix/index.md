# The bundled registry

`rekep` ships one complete FIX registry -- the dictionary every parse types
against -- as JSON shards inside the package, under `rekep/_data/fix`.
Importing `rekep` installs it as the **process default**, so no dictionary
path, download or environment variable is needed:

| call | answers |
| --- | --- |
| `rekep.FixRegistry.from_env()` | the process default registry |
| `rekep.FixCodec.from_env(**pins)` | a codec over it, the pins forwarded to the native codec |
| `rekep.FixRegistry.install_env(registry)` | installs a process default where none is resolved yet; refuses a second one |

```python
from rekep import FixCodec, FixRegistry

registry = FixRegistry.from_env()
codec = FixCodec.from_env(threads=2)

assert len(registry) == 7789
assert codec.registry == registry

message = next(iter(codec.parse_line(b"Sending : 8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|")))
assert message.by_name("symbol").as_py() == "AAPL"
assert message.by_tag(38).as_py() == 12
```

`parse_line` answers a message per frame a line carries, here one, typed by
the registry: `OrderQty(38)` is an exact decimal. [Decode](decode.md) has the
rules and [Encode](encode.md) the way back.

## What it holds

| shape | count | read by |
| --- | ---: | --- |
| definitions in all | 7,789 | `len(registry)` |
| scalar fields | 6,280 | iterating the registry |
| components | 928 | the `components` array of `registry.into_json()` |
| repeating groups | 581 | the `groups` array of the same document |
| message types | 181 | the components carrying `FIX:msgtype`; `registry.msgtype("D")` |
| code sets | 737 | `registry.codeset_names()` |
| crate fields | 40 | `rekep.fix.fix_crate_fields()`, tags 65003 to 65077 |

The crate fields are the columns every registry holds from construction: the
event clocks and identities, the lifecycle `state`, the capture context the
bridge's row header fills, `metadata`, the residual `fixentries`, and the
normalized instrument codes. They are derived facts, not copies of a
specification.

```python
import json

from rekep import FixRegistry
from rekep.fix import fix_crate_fields

registry = FixRegistry.from_env()
document = json.loads(registry.into_json())
tags = [field.fix.tag for field in fix_crate_fields()]

assert sum(1 for _ in registry) == 6280
assert (len(document["components"]), len(document["groups"])) == (928, 581)
assert registry.msgtype("D").name == "newordersingle"
assert len(tags) == 40 and (min(tags), max(tags)) == (65003, 65077)
assert registry.field_by_tag(65052).name == "state"
assert registry.field_by_tag(65053).name == "exprunix"
```

## Code sets

A vocabulary is stored once, as a code set, and a field names the one it
takes in its `fix.codeset`: `Side(54)` names `sidecodeset`, whose codes
translate `buy` and `1` alike to the stored `1`. `registry.codeset(name)`
answers a set's codes, each a `value`, a `name`, a `description` and its
`aliases`.

Two sets are intrinsic to every registry and cannot be changed or removed:

| code set | field | codes |
| --- | --- | --- |
| `statecodeset` | `state` (65052), `int32` | the 60 lifecycle states `rekep.State` enumerates, by code |
| `msgcatcodeset` | `msgcat` (65054), `int32` | the business category of a message type |

```python
from rekep import FixRegistry, State

registry = FixRegistry.from_env()
states = registry.codeset("statecodeset")
side = registry.field_by_tag(54)

assert registry.field_by_name("state").fix.codeset == "statecodeset"
assert [int(code["value"]) for code in states] == [int(state) for state in State]
assert [code["name"] for code in states] == [state.name for state in State]
assert registry.codeset(side.fix.codeset)[0]["name"] == "Buy"
```

`rekep.State` is that code set as a Python `IntEnum`: a stored `state` code
reads back as its member, whose `rank`, `description` and `is_pending()`,
`is_live()`, `is_done()`, `is_cancelled()`, `is_failed()` and
`is_execution()` answer what the code means, and `State.from_fix_status(tag,
code)`, `State.from_fix_msgtype(msgtype)` and `State.from_spelling(text)` read
one off FIX. [States](../tables/states.md) lists the members.

## Another dictionary

The process default is resolved once per process, and importing `rekep`
resolves it: `install_env` then refuses another. Two ways lead to another
dictionary.

**The environment.** The variable `rekep.fix.REGISTRY_VARIABLE` names holds a
dictionary folder. Set in the process environment before `rekep` is imported
-- a scheduler's worker environment, a container's -- it keeps rekep from
installing its own, and `FixRegistry.from_env()` reads that folder instead.

**An explicit codec.** Build the codec over the registry the folder holds and
hand it to the three FIX tasks, and to `deploy`, which types the FIX tables
with it:

```python
import tempfile
from pathlib import Path

from rekep import FixCodec, FixRegistry
from rekep.fix import UNDATED

folder = Path(tempfile.mkdtemp()) / "fix"
FixRegistry.from_env().write_into(folder)

registry = FixRegistry.from_handle(folder)
codec = FixCodec(registry, default_sending_time=UNDATED, threads=4)
assert len(registry) == 7789

try:
    FixRegistry.install_env(registry)
except ValueError as refusal:
    assert "already resolved" in str(refusal)
```

```python
from rekep.pipeline import parse_books, parse_fix_messages_raw, parse_fix_messages_refined

parse_fix_messages_raw(storages, window, codec=codec)
parse_fix_messages_refined(storages, window, codec=codec)
parse_books(storages, window, codec=codec)
```

A folder is the `fields/`, `components/` and `groups/` JSON documents
`FixRegistry.write_into` writes and `FixRegistry.from_handle` reads;
`FixRegistry.from_json` reads the one document `into_json` writes, and
`FixRegistry.from_cfb_file` a bridge configuration file. A folder holding no
specification field is refused wherever a FIX table's shape is built, so an
empty dictionary never types a narrow table. The three tasks must share one
codec: each task after the parse reads a stored row back as the message the
same dictionary wrote.

## Browse it

[Definitions and lookups](registry.md) searches the 7,789 definitions in the
browser, and [Registry assets](assets.md) says how that search is published.
