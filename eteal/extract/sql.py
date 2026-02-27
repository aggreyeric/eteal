"""SQL-based data extraction using SQLAlchemy."""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import Engine, MetaData, Table, select, text
from sqlalchemy import create_engine as _sa_create_engine


class SqlExtractor:
    """Extract data from a SQL database into a :class:`pandas.DataFrame`.

    Parameters
    ----------
    engine:
        A SQLAlchemy :class:`~sqlalchemy.engine.Engine` instance **or** a
        connection URL string (e.g. ``"mssql+pyodbc://…"``).

    Examples
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("sqlite:///:memory:")
    >>> extractor = SqlExtractor(engine)
    >>> df = extractor.extract("SELECT 1 AS value")
    """

    def __init__(self, engine: Engine | str) -> None:
        if isinstance(engine, str):
            engine = _sa_create_engine(engine)
        self._engine: Engine = engine

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute *query* and return the result as a DataFrame.

        Parameters
        ----------
        query:
            Raw SQL string.  Named bind parameters are supported via
            ``":name"`` placeholders when *params* is provided.
        params:
            Optional mapping of parameter names to values.

        Returns
        -------
        pandas.DataFrame
        """
        with self._engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return pd.DataFrame(result.fetchall(), columns=list(result.keys()))

    def extract_table(self, table_name: str, schema: str | None = None) -> pd.DataFrame:
        """Return all rows from *table_name* as a DataFrame.

        Uses SQLAlchemy table reflection so that *table_name* and *schema*
        are never interpolated into a raw SQL string, preventing SQL injection.

        Parameters
        ----------
        table_name:
            Name of the table to read.
        schema:
            Optional database schema name.
        """
        metadata = MetaData()
        table = Table(table_name, metadata, schema=schema, autoload_with=self._engine)
        with self._engine.connect() as conn:
            result = conn.execute(select(table))
            return pd.DataFrame(result.fetchall(), columns=list(result.keys()))

    @property
    def engine(self) -> Engine:
        """The underlying SQLAlchemy engine."""
        return self._engine
