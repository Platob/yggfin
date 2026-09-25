"""`Self`, resolved at runtime on every supported interpreter."""

from __future__ import annotations

import sys

# `Self` is 3.11's, and a hint here is read rather than merely written --
# `get_type_hints` evaluates every one of them -- so the name has to exist at
# runtime on the oldest interpreter this package supports and not only in a
# checker. It is resolved once, here, because this module is already what
# every reader of an annotation imports; nothing else spells the branch, and
# the redundant alias is what says the name is re-exported rather than used.
if sys.version_info >= (3, 11):
    from typing import Self as Self
else:  # pragma: no cover - the back-port is what the older interpreter has
    from typing_extensions import Self as Self
