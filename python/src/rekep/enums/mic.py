"""ISO market identifier code."""

from __future__ import annotations

import enum
import re
from typing import Any

from rekep.enums._ascii import _AsciiInt32


class MIC(_AsciiInt32):
    """ISO 10383 code stored as four ASCII bytes in one `int32`."""

    _PATTERN = enum.nonmember(re.compile(r"^[A-Z0-9]{4}$"))

    UNKNOWN = 0, ""
    """No valid market identifier was present."""

    XOFF = "XOFF"
    """Off-market transaction."""

    XXXX = "XXXX"
    """No market, including an unlisted instrument."""

    @classmethod
    def _valid(cls, text: str) -> bool:
        return bool(cls._PATTERN.fullmatch(text))

    @classmethod
    def schema_metadata(cls) -> dict[str, str]:
        return {**super().schema_metadata(), "pattern": "[A-Z0-9]{4}"}

    @classmethod
    def arrow_from_strings(cls, *values: Any) -> Any:
        """Pack the first valid MIC across string columns with Arrow kernels."""
        if not values:
            raise ValueError("at least one MIC source column is required")
        import pyarrow
        import pyarrow.compute as compute

        alphabet = pyarrow.array(list("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        encoded = []
        for value in values:
            text = compute.utf8_upper(compute.utf8_trim_whitespace(value.cast(pyarrow.string())))
            valid = compute.fill_null(
                compute.match_substring_regex(text, cls._PATTERN.pattern), False
            )
            packed = pyarrow.repeat(pyarrow.scalar(0, pyarrow.int32()), len(text))
            for index, multiplier in enumerate((1 << 24, 1 << 16, 1 << 8, 1)):
                character = compute.utf8_slice_codeunits(text, start=index, stop=index + 1)
                position = compute.index_in(character, value_set=alphabet)
                byte = compute.if_else(
                    compute.less(position, 10),
                    compute.add(position, 48),
                    compute.add(position, 55),
                ).cast(pyarrow.int32())
                packed = compute.add(packed, compute.multiply(byte, multiplier)).cast(
                    pyarrow.int32()
                )
            encoded.append(compute.if_else(valid, packed, pyarrow.scalar(None, pyarrow.int32())))
        return compute.coalesce(*encoded)
