from __future__ import annotations

import daft

from mmops.operators.text import KeywordMatcher, normalize_text


def run_example() -> dict[str, list[object]]:
    """Run a text normalization and keyword annotation pipeline on Daft."""

    df = daft.from_pydict(
        {
            "text": ["  ROBOT navigation  ", None, "manipulation"],
        }
    )
    normalized = df.with_column(
        "normalized",
        normalize_text(df["text"], lowercase=True),
    )
    matcher = KeywordMatcher(
        ["robot", "navigation", "manipulation"],
        whole_word=True,
    )
    return normalized.with_column(
        "keywords",
        matcher.match_batch(normalized["normalized"]),
    ).to_pydict()


if __name__ == "__main__":
    print(run_example())
