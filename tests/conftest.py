"""Shared pytest fixtures for eteal tests."""

from __future__ import annotations

import pytest
import pandas as pd
from sqlalchemy import create_engine, text


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Return a small DataFrame for use in tests."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "Alice"],
            "score": [95.0, 87.5, 92.0, 95.0],
        }
    )


@pytest.fixture
def sqlite_engine():
    """Return an in-memory SQLite engine pre-populated with test data."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE employees (
                    id      INTEGER PRIMARY KEY,
                    name    TEXT NOT NULL,
                    salary  REAL NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO employees (id, name, salary) VALUES "
                "(:id, :name, :salary)"
            ),
            [
                {"id": 1, "name": "Alice", "salary": 70000.0},
                {"id": 2, "name": "Bob", "salary": 85000.0},
                {"id": 3, "name": "Charlie", "salary": 60000.0},
            ],
        )
        conn.commit()
    yield engine
    engine.dispose()
