from __future__ import annotations

from typing import Any

import pyarrow as pa

from ._definitions import BatchOutputError, EntrypointDefinition


def to_arrow_argument(value: Any) -> Any:
    """Convert a Daft Series-like value to one Arrow array.

    Plain Arrow arrays and literal scalar arguments pass through unchanged.
    """

    if isinstance(value, pa.Array):
        return value
    to_arrow = getattr(value, "to_arrow", None)
    if callable(to_arrow):
        converted = to_arrow()
        if not isinstance(converted, pa.Array):
            raise TypeError(
                "batch input to_arrow() must return pyarrow.Array, "
                f"got {type(converted).__name__}"
            )
        return converted
    return value


def _types_compatible(expected: pa.DataType, actual: pa.DataType) -> bool:
    if expected == actual:
        return True
    string_types = {pa.string(), pa.large_string()}
    binary_types = {pa.binary(), pa.large_binary()}
    return (expected in string_types and actual in string_types) or (
        expected in binary_types and actual in binary_types
    )


def validate_batch_output(
    output: Any,
    definition: EntrypointDefinition,
    expected_length: int,
) -> pa.Array:
    """Validate and return a batch entrypoint's Arrow array output."""

    name = definition.original.__qualname__
    if not isinstance(output, pa.Array):
        raise BatchOutputError(
            f"batch entrypoint {name!r} must return pyarrow.Array; "
            f"got {type(output).__name__}"
        )

    expected_dtype = definition.return_dtype
    assert expected_dtype is not None
    if not _types_compatible(expected_dtype, output.type):
        raise BatchOutputError(
            f"batch entrypoint {name!r} declared {expected_dtype} "
            f"but returned {output.type}"
        )
    if len(output) != expected_length:
        raise BatchOutputError(
            f"batch entrypoint {name!r} expected {expected_length} rows "
            f"but returned {len(output)}"
        )
    return output
