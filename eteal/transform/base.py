"""DataFrame transformation utilities."""

from __future__ import annotations

from typing import Any, Callable

import pandas as pd


def drop_duplicates(
    df: pd.DataFrame,
    subset: list[str] | None = None,
    keep: str = "first",
) -> pd.DataFrame:
    """Remove duplicate rows from *df*.

    Parameters
    ----------
    df:
        Input DataFrame.
    subset:
        Column(s) to consider for identifying duplicates.  ``None`` means
        all columns are used.
    keep:
        Which duplicate to keep – ``"first"`` (default), ``"last"``, or
        ``False`` to drop all duplicates.

    Returns
    -------
    pandas.DataFrame
        DataFrame with duplicates removed (index reset).
    """
    return df.drop_duplicates(subset=subset, keep=keep).reset_index(drop=True)


def filter_rows(
    df: pd.DataFrame,
    condition: Callable[[pd.DataFrame], pd.Series],
) -> pd.DataFrame:
    """Keep only the rows where *condition* evaluates to ``True``.

    Parameters
    ----------
    df:
        Input DataFrame.
    condition:
        A callable that receives the DataFrame and returns a boolean
        :class:`pandas.Series`.

    Returns
    -------
    pandas.DataFrame

    Examples
    --------
    >>> filter_rows(df, lambda d: d["age"] > 18)
    """
    mask = condition(df)
    return df[mask].reset_index(drop=True)


def rename_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Rename columns according to *mapping*.

    Parameters
    ----------
    df:
        Input DataFrame.
    mapping:
        ``{old_name: new_name}`` dictionary.

    Returns
    -------
    pandas.DataFrame
    """
    return df.rename(columns=mapping)


def cast_columns(df: pd.DataFrame, dtypes: dict[str, Any]) -> pd.DataFrame:
    """Cast columns to the specified dtypes.

    Parameters
    ----------
    df:
        Input DataFrame.
    dtypes:
        ``{column_name: dtype}`` mapping, e.g. ``{"age": int, "score": float}``.

    Returns
    -------
    pandas.DataFrame
    """
    return df.astype(dtypes)
