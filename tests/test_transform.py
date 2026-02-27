"""Tests for eteal.transform utilities."""

from __future__ import annotations

import pytest
import pandas as pd

from eteal.transform import (
    drop_duplicates,
    filter_rows,
    rename_columns,
    cast_columns,
)


class TestDropDuplicates:
    def test_removes_exact_duplicates(self, sample_df):
        # rows 0 and 3 share the same name and score; use subset to deduplicate
        result = drop_duplicates(sample_df, subset=["name", "score"])
        assert len(result) == 3

    def test_subset_parameter(self, sample_df):
        # duplicate only by name
        result = drop_duplicates(sample_df, subset=["name"])
        assert len(result) == 3

    def test_keep_last(self, sample_df):
        result = drop_duplicates(sample_df, subset=["name", "score"], keep="last")
        assert len(result) == 3
        # The surviving "Alice" row should be the last one (index 3)
        alice_row = result[result["name"] == "Alice"].iloc[0]
        assert alice_row["id"] == 4

    def test_index_reset(self, sample_df):
        result = drop_duplicates(sample_df)
        assert list(result.index) == list(range(len(result)))

    def test_no_duplicates_unchanged(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = drop_duplicates(df)
        assert len(result) == 3


class TestFilterRows:
    def test_filter_by_column_value(self, sample_df):
        result = filter_rows(sample_df, lambda d: d["score"] >= 92)
        assert len(result) == 3
        assert all(result["score"] >= 92)

    def test_filter_removes_all(self, sample_df):
        result = filter_rows(sample_df, lambda d: d["id"] > 1000)
        assert len(result) == 0

    def test_filter_keeps_all(self, sample_df):
        result = filter_rows(sample_df, lambda d: d["id"] > 0)
        assert len(result) == len(sample_df)

    def test_index_reset(self, sample_df):
        result = filter_rows(sample_df, lambda d: d["score"] >= 92)
        assert list(result.index) == list(range(len(result)))


class TestRenameColumns:
    def test_rename_single_column(self, sample_df):
        result = rename_columns(sample_df, {"name": "full_name"})
        assert "full_name" in result.columns
        assert "name" not in result.columns

    def test_rename_multiple_columns(self, sample_df):
        result = rename_columns(sample_df, {"id": "employee_id", "score": "grade"})
        assert "employee_id" in result.columns
        assert "grade" in result.columns

    def test_rename_empty_mapping(self, sample_df):
        result = rename_columns(sample_df, {})
        assert list(result.columns) == list(sample_df.columns)

    def test_rename_does_not_mutate_original(self, sample_df):
        rename_columns(sample_df, {"name": "full_name"})
        assert "name" in sample_df.columns


class TestCastColumns:
    def test_cast_float_to_int(self, sample_df):
        result = cast_columns(sample_df, {"score": int})
        assert result["score"].dtype == int

    def test_cast_int_to_str(self, sample_df):
        result = cast_columns(sample_df, {"id": str})
        assert pd.api.types.is_string_dtype(result["id"])

    def test_cast_multiple_columns(self, sample_df):
        result = cast_columns(sample_df, {"id": str, "score": int})
        assert pd.api.types.is_string_dtype(result["id"])
        assert result["score"].dtype == int

    def test_invalid_cast_raises(self, sample_df):
        with pytest.raises((ValueError, TypeError)):
            cast_columns(sample_df, {"name": int})
