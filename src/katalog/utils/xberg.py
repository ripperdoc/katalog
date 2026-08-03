"""Typing compatibility helpers for the xberg library."""

from __future__ import annotations

import builtins
from dataclasses import dataclass

from xberg import ExtractInput


@dataclass(frozen=True, slots=True)
class BytesExtractInput(ExtractInput):
    """`ExtractInput` with a usable `bytes` constructor argument.

    xberg 1.0.11 annotates the field as `bytes: bytes | None`, where the
    annotation resolves to the field itself instead of the builtin, so type
    checkers see the parameter as `None`-only. Redeclaring the field here
    regenerates a correctly typed `__init__`.
    """

    bytes: builtins.bytes | None = None
