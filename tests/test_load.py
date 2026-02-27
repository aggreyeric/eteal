"""Tests for eteal.load (SqlLoader and MinioLoader)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call
import io

import pytest
import pandas as pd
from sqlalchemy import create_engine, text

from eteal.load import SqlLoader, MinioLoader


# ---------------------------------------------------------------------------
# SqlLoader tests
# ---------------------------------------------------------------------------


class TestSqlLoaderInit:
    def test_accepts_engine_object(self, sqlite_engine):
        loader = SqlLoader(sqlite_engine)
        assert loader.engine is sqlite_engine

    def test_accepts_url_string(self):
        loader = SqlLoader("sqlite:///:memory:")
        assert loader.engine is not None


class TestSqlLoaderLoad:
    def test_load_creates_table_and_returns_row_count(self, sample_df):
        engine = create_engine("sqlite:///:memory:")
        loader = SqlLoader(engine)
        count = loader.load(sample_df, "test_table")
        assert count == len(sample_df)

    def test_load_data_readable(self, sample_df):
        engine = create_engine("sqlite:///:memory:")
        loader = SqlLoader(engine)
        loader.load(sample_df, "scores")

        with engine.connect() as conn:
            rows = conn.execute(text("SELECT COUNT(*) FROM scores")).scalar()
        assert rows == len(sample_df)

    def test_load_append(self, sample_df):
        engine = create_engine("sqlite:///:memory:")
        loader = SqlLoader(engine)
        loader.load(sample_df, "scores", if_exists="replace")
        loader.load(sample_df, "scores", if_exists="append")

        with engine.connect() as conn:
            rows = conn.execute(text("SELECT COUNT(*) FROM scores")).scalar()
        assert rows == len(sample_df) * 2

    def test_load_replace(self, sample_df):
        engine = create_engine("sqlite:///:memory:")
        loader = SqlLoader(engine)
        loader.load(sample_df, "scores", if_exists="replace")
        loader.load(sample_df.head(2), "scores", if_exists="replace")

        with engine.connect() as conn:
            rows = conn.execute(text("SELECT COUNT(*) FROM scores")).scalar()
        assert rows == 2


# ---------------------------------------------------------------------------
# MinioLoader tests  (uses mocked minio.Minio)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_minio(monkeypatch):
    """Patch minio.Minio with a MagicMock so no real server is needed."""
    mock_client = MagicMock()
    mock_client.bucket_exists.return_value = False  # triggers make_bucket

    with patch("eteal.load.minio.Minio", return_value=mock_client):
        loader = MinioLoader("localhost:9000", "minioadmin", "minioadmin")

    return loader, mock_client


class TestMinioLoaderEnsureBucket:
    def test_creates_bucket_when_missing(self, mock_minio):
        loader, client = mock_minio
        client.bucket_exists.return_value = False
        loader.ensure_bucket("my-bucket")
        client.make_bucket.assert_called_once_with("my-bucket")

    def test_skips_creation_when_bucket_exists(self, mock_minio):
        loader, client = mock_minio
        client.bucket_exists.return_value = True
        loader.ensure_bucket("my-bucket")
        client.make_bucket.assert_not_called()


class TestMinioLoaderLoadCsv:
    def test_puts_csv_object(self, mock_minio, sample_df):
        loader, client = mock_minio
        client.bucket_exists.return_value = True  # bucket already exists

        result = loader.load_csv(sample_df, "my-bucket", "out/data.csv")

        assert result == "my-bucket/out/data.csv"
        client.put_object.assert_called_once()
        args = client.put_object.call_args
        assert args[0][0] == "my-bucket"
        assert args[0][1] == "out/data.csv"

    def test_ensures_bucket_by_default(self, mock_minio, sample_df):
        loader, client = mock_minio
        client.bucket_exists.return_value = False

        loader.load_csv(sample_df, "new-bucket", "data.csv")

        client.make_bucket.assert_called_once_with("new-bucket")

    def test_skip_ensure_bucket(self, mock_minio, sample_df):
        loader, client = mock_minio
        loader.load_csv(sample_df, "bucket", "data.csv", ensure_bucket=False)
        client.bucket_exists.assert_not_called()


class TestMinioLoaderLoadParquet:
    def test_puts_parquet_object(self, mock_minio, sample_df):
        loader, client = mock_minio
        client.bucket_exists.return_value = True

        result = loader.load_parquet(sample_df, "my-bucket", "out/data.parquet")

        assert result == "my-bucket/out/data.parquet"
        client.put_object.assert_called_once()
        args = client.put_object.call_args
        assert args[0][0] == "my-bucket"
        assert args[0][1] == "out/data.parquet"


class TestMinioLoaderUploadFile:
    def test_upload_local_file(self, mock_minio, tmp_path):
        loader, client = mock_minio
        client.bucket_exists.return_value = True

        local_file = tmp_path / "report.csv"
        local_file.write_text("a,b\n1,2\n")

        result = loader.upload_file(local_file, "bucket", "report.csv")

        assert result == "bucket/report.csv"
        client.fput_object.assert_called_once_with(
            "bucket", "report.csv", str(local_file)
        )

    def test_upload_uses_filename_as_default_object_name(self, mock_minio, tmp_path):
        loader, client = mock_minio
        client.bucket_exists.return_value = True

        local_file = tmp_path / "auto_name.csv"
        local_file.write_text("x\n1\n")

        loader.upload_file(local_file, "bucket")

        client.fput_object.assert_called_once_with(
            "bucket", "auto_name.csv", str(local_file)
        )
