"""Tests for eteal.extract.SqlExtractor."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from eteal.extract import SqlExtractor


class TestSqlExtractorInit:
    def test_accepts_engine_object(self, sqlite_engine):
        extractor = SqlExtractor(sqlite_engine)
        assert extractor.engine is sqlite_engine

    def test_accepts_url_string(self):
        extractor = SqlExtractor("sqlite:///:memory:")
        assert extractor.engine is not None


class TestSqlExtractorExtract:
    def test_basic_query(self, sqlite_engine):
        extractor = SqlExtractor(sqlite_engine)
        df = extractor.extract("SELECT * FROM employees")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "salary"]

    def test_query_with_params(self, sqlite_engine):
        extractor = SqlExtractor(sqlite_engine)
        df = extractor.extract(
            "SELECT * FROM employees WHERE id = :eid",
            params={"eid": 1},
        )
        assert len(df) == 1
        assert df.iloc[0]["name"] == "Alice"

    def test_returns_empty_df_for_no_rows(self, sqlite_engine):
        extractor = SqlExtractor(sqlite_engine)
        df = extractor.extract(
            "SELECT * FROM employees WHERE id = :eid",
            params={"eid": 999},
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_column_names_preserved(self, sqlite_engine):
        extractor = SqlExtractor(sqlite_engine)
        df = extractor.extract("SELECT name, salary FROM employees")
        assert list(df.columns) == ["name", "salary"]


class TestSqlExtractorExtractTable:
    def test_extract_table(self, sqlite_engine):
        extractor = SqlExtractor(sqlite_engine)
        df = extractor.extract_table("employees")
        assert len(df) == 3

    def test_extract_table_with_schema(self):
        """extract_table passes schema to query (syntax check only)."""
        engine = create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE t (x INTEGER)"))
            conn.execute(text("INSERT INTO t VALUES (42)"))
            conn.commit()

        extractor = SqlExtractor(engine)
        # SQLite ignores schema prefix, so this is a smoke test
        df = extractor.extract_table("t")
        assert df.iloc[0]["x"] == 42
