from __future__ import annotations

import daft
import pyarrow as pa
import pytest

import mmops.operators as operators
from mmops.operators.text import KeywordMatcher


def test_keyword_matcher_is_exported_from_operator_catalog() -> None:
    assert operators.KeywordMatcher is KeywordMatcher


def test_keyword_matcher_validates_and_deduplicates_configuration() -> None:
    with pytest.raises(ValueError, match="at least one"):
        KeywordMatcher([]).match("text")

    with pytest.raises(ValueError, match="non-empty"):
        KeywordMatcher(["ray", ""]).match("text")

    with pytest.raises(ValueError, match="duplicate"):
        KeywordMatcher(["Ray", "ray"]).match("text")


def test_keyword_matcher_eager_row_preserves_keyword_order() -> None:
    matcher = KeywordMatcher(["Ray", "Daft", "data"])

    assert matcher.match("DAFT runs data on Ray; data repeats") == [
        "Ray",
        "Daft",
        "data",
    ]
    assert matcher.match("nothing") == []
    assert matcher.match(None) is None


def test_keyword_matcher_supports_case_sensitive_whole_word_matching() -> None:
    matcher = KeywordMatcher(
        ["Ray", "ray"],
        case_sensitive=True,
        whole_word=True,
    )

    assert matcher.match("Ray ray XRay ray2") == ["Ray", "ray"]


def test_keyword_matcher_eager_batch_matches_row_null_semantics() -> None:
    matcher = KeywordMatcher(["Ray", "Daft"])
    values = pa.array(["Daft on Ray", None, "other"], type=pa.string())

    result = matcher.match_batch(values)

    assert isinstance(result, pa.Array)
    assert result.type == pa.list_(pa.string())
    assert result.to_pylist() == [["Ray", "Daft"], None, []]

    empty = matcher.match_batch(pa.array([], type=pa.string()))
    assert empty.type == pa.list_(pa.string())
    assert empty.to_pylist() == []


def test_keyword_matcher_row_and_batch_run_in_real_daft_dataframes() -> None:
    matcher = KeywordMatcher(["Ray", "Daft"])
    df = daft.from_pydict({"text": ["Daft on Ray", None, "other"]})
    expected = [["Ray", "Daft"], None, []]

    row_result = df.select(matcher.match(df["text"]).alias("matches")).to_pydict()
    batch_result = df.select(
        matcher.match_batch(df["text"]).alias("matches")
    ).to_pydict()

    assert row_result == {"matches": expected}
    assert batch_result == {"matches": expected}


def test_keyword_matcher_handle_is_pickleable_and_hides_helpers() -> None:
    matcher = KeywordMatcher(["Ray"])
    restored = daft.pickle.loads(daft.pickle.dumps(matcher))

    assert restored.match("ray") == ["Ray"]
    with pytest.raises(AttributeError, match="_match_text"):
        getattr(restored, "_match_text")
