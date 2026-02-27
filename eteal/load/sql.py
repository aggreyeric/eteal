"""SQL-based data loading using SQLAlchemy."""

from __future__ import annotations

from typing import Literal

import pandas as pd
from sqlalchemy import Engine
from sqlalchemy import create_engine as _sa_create_engine


class SqlLoader:
    """Load a :class:`pandas.DataFrame` into a SQL table.

    Parameters
    ----------
    engine:
        A SQLAlchemy :class:`~sqlalchemy.engine.Engine` instance **or** a
        connection URL string.

    Examples
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("sqlite:///:memory:")
    >>> loader = SqlLoader(engine)
    >>> loader.load(df, "my_table")
    """

    def __init__(self, engine: Engine | str) -> None:
        if isinstance(engine, str):
            engine = _sa_create_engine(engine)
        self._engine: Engine = engine

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str | None = None,
        if_exists: Literal["fail", "replace", "append"] = "append",
        chunksize: int | None = None,
        index: bool = False,
    ) -> int:
        """Write *df* to *table_name*.

        Parameters
        ----------
        df:
            Data to write.
        table_name:
            Destination table name.
        schema:
            Optional database schema.
        if_exists:
            Behaviour if the table already exists:
            ``"fail"`` (default raises), ``"replace"`` (drop + recreate),
            or ``"append"`` (insert rows).
        chunksize:
            Optional number of rows to write per batch.
        index:
            Whether to write the DataFrame index as a column.

        Returns
        -------
        int
            Number of rows written.
        """
        df.to_sql(
            name=table_name,
            con=self._engine,
            schema=schema,
            if_exists=if_exists,
            chunksize=chunksize,
            index=index,
        )
        return len(df)

    @property
    def engine(self) -> Engine:
        """The underlying SQLAlchemy engine."""
        return self._engine
