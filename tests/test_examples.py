from __future__ import annotations

from examples.text_pipeline import run_example


def test_text_pipeline_example_runs_against_daft() -> None:
    assert run_example() == {
        "text": ["  ROBOT navigation  ", None, "manipulation"],
        "normalized": ["robot navigation", None, "manipulation"],
        "keywords": [
            ["robot", "navigation"],
            None,
            ["manipulation"],
        ],
    }
