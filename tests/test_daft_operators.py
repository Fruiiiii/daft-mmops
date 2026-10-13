from __future__ import annotations

import asyncio

import daft
import pyarrow as pa
import pyarrow.compute as pc
import pytest

import mmops


def test_stateful_operator_reuses_constructor_state_for_row_entrypoint() -> None:
    @mmops.operator
    class Multiplier:
        def __init__(self, factor: int) -> None:
            self.factor = factor

        def helper(self, value: int) -> int:
            return value * self.factor

        @mmops.row
        def multiply(self, value: int) -> int:
            return self.helper(value)

    multiplier = Multiplier(3)
    result = daft.from_pydict({"value": [1, 2, 3]}).select(
        multiplier.multiply(daft.col("value")).alias("result")
    )

    assert result.to_pydict() == {"result": [3, 6, 9]}
    with pytest.raises(AttributeError, match="helper"):
        getattr(multiplier, "helper")


def test_one_operator_can_expose_row_and_batch_entrypoints() -> None:
    @mmops.operator
    class Offsetter:
        def __init__(self, offset: int) -> None:
            self.offset = offset

        @mmops.row
        def add_one(self, value: int) -> int:
            return value + self.offset

        @mmops.batch(return_dtype=pa.int64(), batch_size=2)
        def add_batch(self, values: pa.Array) -> pa.Array:
            return pc.add(values, self.offset)

    offsetter = Offsetter(10)
    df = daft.from_pydict({"value": [1, 2, 3]})

    assert df.select(offsetter.add_one(df["value"]).alias("result")).to_pydict() == {
        "result": [11, 12, 13]
    }
    assert df.select(offsetter.add_batch(df["value"]).alias("result")).to_pydict() == {
        "result": [11, 12, 13]
    }


def test_operator_supports_async_row_and_batch_methods() -> None:
    @mmops.operator(max_concurrency=2)
    class AsyncOffsetter:
        def __init__(self, offset: int) -> None:
            self.offset = offset

        @mmops.row
        async def add_one(self, value: int) -> int:
            await asyncio.sleep(0)
            return value + self.offset

        @mmops.batch(return_dtype=pa.int64(), batch_size=2)
        async def add_batch(self, values: pa.Array) -> pa.Array:
            await asyncio.sleep(0)
            return pc.add(values, self.offset)

    offsetter = AsyncOffsetter(5)
    df = daft.from_pydict({"value": [1, 2, 3]})

    row_result = df.select(offsetter.add_one(df["value"]).alias("result")).to_pydict()
    batch_result = df.select(
        offsetter.add_batch(df["value"]).alias("result")
    ).to_pydict()

    assert sorted(row_result["result"]) == [6, 7, 8]
    assert sorted(batch_result["result"]) == [6, 7, 8]


def test_callable_entrypoint_is_forwarded_by_operator_instance() -> None:
    @mmops.operator
    class Doubler:
        @mmops.row
        def __call__(self, value: int) -> int:
            return value * 2

    doubler = Doubler()
    result = daft.from_pydict({"value": [2, 4]}).select(
        doubler(daft.col("value")).alias("result")
    )

    assert result.to_pydict() == {"result": [4, 8]}


def test_constructed_operator_handle_round_trips_through_daft_pickle() -> None:
    @mmops.operator
    class Offsetter:
        def __init__(self, offset: int) -> None:
            self.offset = offset

        @mmops.row
        def add(self, value: int) -> int:
            return value + self.offset

    restored = daft.pickle.loads(daft.pickle.dumps(Offsetter(4)))
    result = daft.from_pydict({"value": [1, 2]}).select(
        restored.add(daft.col("value")).alias("result")
    )

    assert result.to_pydict() == {"result": [5, 6]}
