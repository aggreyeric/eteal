"""MinIO / S3-compatible object-storage loader."""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
from minio import Minio
from minio.error import S3Error


class MinioLoader:
    """Load data into a MinIO (S3-compatible) bucket.

    Parameters
    ----------
    endpoint:
        MinIO server endpoint, e.g. ``"localhost:9000"``.
    access_key:
        Access key (username).
    secret_key:
        Secret key (password).
    secure:
        Use HTTPS.  Defaults to ``False`` for local development.

    Examples
    --------
    >>> loader = MinioLoader("localhost:9000", "minioadmin", "minioadmin")
    >>> loader.load_csv(df, "my-bucket", "output/data.csv")
    """

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
    ) -> None:
        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    # ------------------------------------------------------------------
    # Bucket helpers
    # ------------------------------------------------------------------

    def ensure_bucket(self, bucket_name: str) -> None:
        """Create *bucket_name* if it does not already exist."""
        if not self._client.bucket_exists(bucket_name):
            self._client.make_bucket(bucket_name)

    # ------------------------------------------------------------------
    # Upload helpers
    # ------------------------------------------------------------------

    def load_csv(
        self,
        df: pd.DataFrame,
        bucket_name: str,
        object_name: str,
        *,
        ensure_bucket: bool = True,
        **to_csv_kwargs,
    ) -> str:
        """Serialise *df* as CSV and upload to MinIO.

        Parameters
        ----------
        df:
            DataFrame to upload.
        bucket_name:
            Target bucket.
        object_name:
            Object path inside the bucket, e.g. ``"output/2024/data.csv"``.
        ensure_bucket:
            Automatically create the bucket if it does not exist.
        **to_csv_kwargs:
            Extra keyword arguments forwarded to :meth:`pandas.DataFrame.to_csv`.

        Returns
        -------
        str
            The full ``bucket_name/object_name`` path.
        """
        if ensure_bucket:
            self.ensure_bucket(bucket_name)

        to_csv_kwargs.setdefault("index", False)
        csv_bytes = df.to_csv(**to_csv_kwargs).encode()
        buffer = io.BytesIO(csv_bytes)

        self._client.put_object(
            bucket_name,
            object_name,
            buffer,
            length=len(csv_bytes),
            content_type="text/csv",
        )
        return f"{bucket_name}/{object_name}"

    def load_parquet(
        self,
        df: pd.DataFrame,
        bucket_name: str,
        object_name: str,
        *,
        ensure_bucket: bool = True,
        **to_parquet_kwargs,
    ) -> str:
        """Serialise *df* as Parquet and upload to MinIO.

        Parameters
        ----------
        df:
            DataFrame to upload.
        bucket_name:
            Target bucket.
        object_name:
            Object path inside the bucket.
        ensure_bucket:
            Automatically create the bucket if it does not exist.
        **to_parquet_kwargs:
            Extra keyword arguments forwarded to :meth:`pandas.DataFrame.to_parquet`.

        Returns
        -------
        str
            The full ``bucket_name/object_name`` path.
        """
        if ensure_bucket:
            self.ensure_bucket(bucket_name)

        to_parquet_kwargs.setdefault("index", False)
        buffer = io.BytesIO()
        df.to_parquet(buffer, **to_parquet_kwargs)
        parquet_bytes = buffer.getvalue()

        self._client.put_object(
            bucket_name,
            object_name,
            io.BytesIO(parquet_bytes),
            length=len(parquet_bytes),
            content_type="application/octet-stream",
        )
        return f"{bucket_name}/{object_name}"

    def upload_file(
        self,
        local_path: str | Path,
        bucket_name: str,
        object_name: str | None = None,
        *,
        ensure_bucket: bool = True,
    ) -> str:
        """Upload a local file to MinIO.

        Parameters
        ----------
        local_path:
            Path to the local file.
        bucket_name:
            Target bucket.
        object_name:
            Object name inside the bucket.  Defaults to the file name.
        ensure_bucket:
            Automatically create the bucket if it does not exist.

        Returns
        -------
        str
            The full ``bucket_name/object_name`` path.
        """
        local_path = Path(local_path)
        if object_name is None:
            object_name = local_path.name

        if ensure_bucket:
            self.ensure_bucket(bucket_name)

        self._client.fput_object(bucket_name, object_name, str(local_path))
        return f"{bucket_name}/{object_name}"

    @property
    def client(self) -> Minio:
        """The underlying :class:`minio.Minio` client."""
        return self._client
