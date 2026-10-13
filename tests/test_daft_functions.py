from __future__ import annotations

import asyncio

import daft
import pyarrow as pa
import pyarrow.compute as pc

import mmops


def test_sync_row_function_runs_in_a_real_daft_dataframe() -> None:
    @mmops.row
    def normalize(text: str) -> str:
        return text.strip().lower()

    df = daft.from_pydict({"text": [" Hello ", "WORLD"]})
    result = df.with_column("normalized", normalize(df["text"])).to_pydict()

    assert result == {
        "text": [" Hello ", "WORLD"],
        "normalized": ["hello", "world"],
    }


def test_row_function_accepts_explicit_arrow_return_dtype() -> None:
    @mmops.row(return_dtype=pa.float32())
    def score(text: str) -> float:
        return float(len(text))

    result = daft.from_pydict({"text": ["a", "abc"]}).select(
        score(daft.col("text")).alias("score")
    )

    assert result.schema()["score"].dtype == daft.DataType.float32()
    assert result.to_pydict() == {"score": [1.0, 3.0]}


def test_async_row_function_is_awaited_by_daft() -> None:
    @mmops.row
    async def add_one(value: int) -> int:
        await asyncio.sleep(0)
        return value + 1

    result = daft.from_pydict({"value": [1, 2]}).select(
        add_one(daft.col("value")).alias("result")
    )

    assert result.to_pydict() == {"result": [2, 3]}


def test_sync_batch_function_receives_arrow_arrays_and_literal_arguments() -> None:
    observed: list[tuple[type, type, int]] = []

    @mmops.batch(return_dtype=pa.int64(), batch_size=2)
    def add(a: pa.Array, b: pa.Array, offset: int) -> pa.Array:
        observed.append((type(a), type(b), len(a)))
        return pc.add(pc.add(a, b), offset)

    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
    result = df.select(add(df["a"], df["b"], 7).alias("sum")).to_pydict()

    assert result == {"sum": [18, 29, 40, 51, 62]}
    assert observed
    assert all(a_type is pa.Int64Array for a_type, _, _ in observed)
    assert all(b_type is pa.Int64Array for _, b_type, _ in observed)
    assert max(length for _, _, length in observed) <= 2


def test_async_batch_function_is_awaited_and_returns_arrow() -> None:
    @mmops.batch(return_dtype=pa.int64(), batch_size=2)
    async def double(values: pa.Array) -> pa.Array:
        await asyncio.sleep(0)
        return pc.multiply(values, 2)

    result = daft.from_pydict({"value": [1, 2, 3]}).select(
        double(daft.col("value")).alias("result")
    )

    assert sorted(result.to_pydict()["result"]) == [2, 4, 6]


def test_empty_dataframe_does_not_break_batch_contract() -> None:
    @mmops.batch(return_dtype=pa.int64(), batch_size=2)
    def identity(values: pa.Array) -> pa.Array:
        return values

    result = daft.from_pydict({"value": pa.array([], type=pa.int64())}).select(
        identity(daft.col("value")).alias("result")
    )

    assert result.to_pydict() == {"result": []}
