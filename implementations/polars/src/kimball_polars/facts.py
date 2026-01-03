"""Fact table helpers with temporal SCD2 joins and FK defaults."""

import polars as pl


def temporal_scd2_join(
    fact_df: pl.DataFrame,
    dim_df: pl.DataFrame,
    fact_date_col: str,
    dim_natural_key: str,
    fact_natural_key: str,
    dim_surrogate_key: str,
    valid_from_col: str = "__valid_from",
    valid_to_col: str = "__valid_to",
) -> pl.DataFrame:
    """
    Join fact to SCD2 dimension using temporal matching.

    Finds the dimension row where:
        valid_from <= fact_date < valid_to

    Args:
        fact_df: Fact table DataFrame
        dim_df: SCD2 dimension DataFrame
        fact_date_col: Date column in fact table
        dim_natural_key: Natural key column in dimension
        fact_natural_key: Corresponding FK column in fact
        dim_surrogate_key: Surrogate key to retrieve from dimension
        valid_from_col: SCD2 valid_from column name
        valid_to_col: SCD2 valid_to column name

    Returns:
        Fact DataFrame with surrogate key added
    """
    # Cast date column if needed (handle string dates)
    date_col = pl.col(fact_date_col)
    if fact_df[fact_date_col].dtype == pl.String:
        date_col = pl.col(fact_date_col).str.to_datetime(
            format="%Y-%m-%d", strict=False
        )

    fact_with_dt = fact_df.with_columns(date_col.alias("__fact_dt"))

    # Cross join and filter to matching time range
    result = (
        fact_with_dt.join(
            dim_df.select(
                [dim_natural_key, dim_surrogate_key, valid_from_col, valid_to_col]
            ),
            left_on=fact_natural_key,
            right_on=dim_natural_key,
            how="left",
        )
        .filter(
            (pl.col("__fact_dt") >= pl.col(valid_from_col))
            & (pl.col("__fact_dt") < pl.col(valid_to_col))
        )
        .drop(["__fact_dt", valid_from_col, valid_to_col])
    )

    return result


def fill_unknown_fk(
    df: pl.DataFrame,
    fk_column: str,
    default_value: int = -1,
) -> pl.DataFrame:
    """
    Fill NULL foreign keys with Kimball-style default value.

    Args:
        df: DataFrame with FK column
        fk_column: Name of the FK column
        default_value: Value for unknown dimension (typically -1)

    Returns:
        DataFrame with NULLs replaced by default
    """
    return df.with_columns(pl.col(fk_column).fill_null(default_value))


def build_fact_table(
    source_df: pl.DataFrame,
    dimension_joins: list[dict],
    fk_defaults: dict[str, int] | None = None,
) -> pl.DataFrame:
    """
    Build a fact table with dimension lookups and FK defaults.

    Args:
        source_df: Source transaction data
        dimension_joins: List of join specs, each containing:
            - dim_df: Dimension DataFrame
            - fact_key: FK column in fact
            - dim_key: NK column in dimension
            - sk_col: Surrogate key to retrieve
            - temporal: If True, use temporal SCD2 join
            - date_col: (if temporal) Date column for matching
        fk_defaults: Dict of {fk_column: default_value} for unknown dims

    Returns:
        Fact table with surrogate keys
    """
    result = source_df

    for join_spec in dimension_joins:
        dim_df = join_spec["dim_df"]
        fact_key = join_spec["fact_key"]
        dim_key = join_spec["dim_key"]
        sk_col = join_spec["sk_col"]

        if join_spec.get("temporal", False):
            date_col = join_spec["date_col"]
            result = temporal_scd2_join(
                result, dim_df, date_col, dim_key, fact_key, sk_col
            )
        else:
            # Simple current-row join
            dim_subset = dim_df.select([dim_key, sk_col])
            if "__is_current" in dim_df.columns:
                dim_subset = dim_df.filter(pl.col("__is_current")).select(
                    [dim_key, sk_col]
                )

            result = result.join(
                dim_subset, left_on=fact_key, right_on=dim_key, how="left"
            )

    # Apply FK defaults
    if fk_defaults:
        for fk_col, default_val in fk_defaults.items():
            if fk_col in result.columns:
                result = fill_unknown_fk(result, fk_col, default_val)

    return result
