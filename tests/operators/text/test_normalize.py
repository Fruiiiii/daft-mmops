from __future__ import annotations

import daft
import pyarrow as pa
import pytest

import mmops.operators as operators
from mmops.operators.text import normalize_text


def test_normalize_text_is_exported_from_operator_catalog() -> None:
    assert operators.normalize_text is normalize_text


def test_normalize_text_uses_arrow_kernels_and_preserves_nulls() -> None:
    values = pa.array(
        ["  Ｈｅｌｌｏ\t WORLD  ", None, "A\n B"],
        type=pa.string(),
    )

    result = normalize_text(values, lowercase=True)

    assert isinstance(result, pa.Array)
    assert result.type == pa.string()
    assert result.to_pylist() == ["hello world", None, "a b"]


def test_normalize_text_options_are_explicit() -> None:
    values = pa.array(["  A\t B  "], type=pa.string())

    result = normalize_text(
        values,
        form="NFC",
        lowercase=False,
        collapse_whitespace=False,
        strip=False,
    )

    assert result.to_pylist() == ["  A\t B  "]


def test_normalize_text_rejects_unknown_unicode_form() -> None:
    with pytest.raises(ValueError, match="normalization form"):
        normalize_text(pa.array(["text"]), form="invalid")


def test_normalize_text_accepts_an_empty_string_array() -> None:
    result = normalize_text(pa.array([], type=pa.string()))

    assert result.type == pa.string()
    assert result.to_pylist() == []


def test_normalize_text_runs_in_a_real_daft_dataframe() -> None:
    df = daft.from_pydict(
        {
            "text": ["  Ｈｅｌｌｏ\t WORLD  ", None, "A\n B"],
        }
    )

    result = df.select(
        normalize_text(df["text"], lowercase=True).alias("normalized")
    ).to_pydict()

    assert result == {"normalized": ["hello world", None, "a b"]}
