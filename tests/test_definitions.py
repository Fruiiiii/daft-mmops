from __future__ import annotations

import inspect
import pickle
import subprocess
import sys

import pyarrow as pa
import pytest

import mmops


def _pickleable_identity(value: int) -> int:
    return value


def test_importing_mmops_does_not_import_daft() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import mmops; print('daft' in sys.modules)",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == "False"


def test_row_supports_bare_decorator_and_preserves_callable_metadata() -> None:
    @mmops.row
    def normalize(text: str) -> str:
        """Normalize text."""
        return text.strip().lower()

    assert normalize.__name__ == "normalize"
    assert normalize.__doc__ == "Normalize text."
    assert str(inspect.signature(normalize)) == "(text: 'str') -> 'str'"
    assert normalize.definition.kind == "row"
    assert normalize.definition.return_dtype is None
    assert normalize.definition.original("  HELLO ") == "hello"


def test_row_accepts_an_explicit_arrow_return_dtype() -> None:
    @mmops.row(
        return_dtype=pa.float32(),
        resources=mmops.Resources(cpus=0.5),
    )
    def score(text: str) -> float:
        return float(len(text))

    assert score.definition.return_dtype == pa.float32()
    assert score.definition.resources == mmops.Resources(cpus=0.5)


def test_batch_requires_an_arrow_return_dtype() -> None:
    with pytest.raises(mmops.DefinitionError, match="return_dtype"):

        @mmops.batch
        def invalid(values: pa.Array) -> pa.Array:
            return values


def test_batch_keeps_resource_requirements() -> None:
    @mmops.batch(
        return_dtype=pa.int64(),
        resources=mmops.Resources(cpus=2, memory_bytes=1024),
    )
    def identity(values: pa.Array) -> pa.Array:
        return values

    assert identity.definition.resources == mmops.Resources(
        cpus=2,
        memory_bytes=1024,
    )


def test_definition_options_are_deeply_immutable_and_pickleable() -> None:
    caller_options = {
        "daft": {
            "ray_options": {
                "labels": ["gpu", "inference"],
            }
        }
    }

    definition = mmops.EntrypointDefinition(
        original=_pickleable_identity,
        kind="row",
        backend_options=caller_options,
    )

    caller_options["daft"]["ray_options"]["labels"].append("mutated")
    restored = pickle.loads(pickle.dumps(definition))

    assert restored.backend_options["daft"]["ray_options"]["labels"] == (
        "gpu",
        "inference",
    )
    with pytest.raises(TypeError):
        restored.backend_options["daft"]["new"] = True


def test_batch_rejects_non_arrow_return_dtype() -> None:
    with pytest.raises(mmops.DefinitionError, match="pyarrow.DataType"):

        @mmops.batch(return_dtype=list[int])
        def invalid(values: pa.Array) -> pa.Array:
            return values


@pytest.mark.parametrize("batch_size", [0, -1, 1.5, True])
def test_batch_size_must_be_a_positive_integer(batch_size: object) -> None:
    with pytest.raises(mmops.DefinitionError, match="batch_size"):

        @mmops.batch(return_dtype=pa.int64(), batch_size=batch_size)
        def invalid(values: pa.Array) -> pa.Array:
            return values


def test_invalid_error_policy_is_rejected() -> None:
    with pytest.raises(mmops.DefinitionError, match="on_error"):

        @mmops.row(on_error="swallow")
        def invalid(value: int) -> int:
            return value


def test_operator_collects_only_explicit_row_and_batch_entrypoints() -> None:
    @mmops.operator(resources=mmops.Resources(gpus=1), max_concurrency=4)
    class Model:
        def helper(self, value: str) -> str:
            return value.upper()

        @mmops.row
        def predict_one(self, value: str) -> str:
            return self.helper(value)

        @mmops.batch(return_dtype=pa.string(), batch_size=16)
        def predict_batch(self, values: pa.Array) -> pa.Array:
            return values

    definition = Model.definition

    assert definition.original.__name__ == "Model"
    assert definition.resources == mmops.Resources(gpus=1)
    assert definition.max_concurrency == 4
    assert set(definition.entrypoints) == {"predict_one", "predict_batch"}
    assert definition.entrypoints["predict_one"].kind == "row"
    assert definition.entrypoints["predict_batch"].kind == "batch"


def test_operator_requires_at_least_one_explicit_entrypoint() -> None:
    with pytest.raises(mmops.DefinitionError, match="at least one"):

        @mmops.operator
        class InvalidOperator:
            def helper(self) -> None:
                pass


def test_resource_values_must_be_non_negative() -> None:
    with pytest.raises(mmops.DefinitionError, match="gpus"):
        mmops.Resources(gpus=-1)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cpus": True},
        {"gpus": float("nan")},
        {"memory_bytes": 1.5},
    ],
)
def test_resource_values_have_valid_numeric_types(kwargs: dict[str, object]) -> None:
    with pytest.raises(mmops.DefinitionError):
        mmops.Resources(**kwargs)


@pytest.mark.parametrize("max_retries", [1.5, True])
def test_max_retries_must_be_a_non_negative_integer(max_retries: object) -> None:
    with pytest.raises(mmops.DefinitionError, match="max_retries"):

        @mmops.row(max_retries=max_retries)
        def invalid(value: int) -> int:
            return value


@pytest.mark.parametrize("max_concurrency", [1.5, True])
def test_max_concurrency_must_be_a_positive_integer(
    max_concurrency: object,
) -> None:
    with pytest.raises(mmops.DefinitionError, match="max_concurrency"):

        @mmops.operator(max_concurrency=max_concurrency)
        class InvalidOperator:
            @mmops.row
            def run(self, value: int) -> int:
                return value
