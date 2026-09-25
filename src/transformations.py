"""
Pure PySpark transformation logic for the customer shopping behaviour pipeline.

Deliberately kept independent of `awsglue` / `DynamicFrame` so it can be unit
tested with a plain local SparkSession -- no Glue container, no AWS
credentials, no network access required. The Glue job entry point
(glue_jobs/CleaningAndTransforming.py) imports and calls `clean_and_transform`
directly; this module is also uploaded standalone via Glue's
`--extra-py-files` so it is importable at runtime as `transformations`.
"""
from __future__ import annotations

import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType

logger = logging.getLogger(__name__)

# Columns that together uniquely identify a single shopping event. Used to
# de-duplicate on the natural key rather than the full row, so two events
# that differ only in an incidental column (e.g. a re-ingested partition
# column) still collapse to one.
DEDUP_KEY_COLUMNS: List[str] = [
    "event_time",
    "event_type",
    "product_id",
    "user_id",
    "user_session",
]

# Columns without which an event can't be attributed to a product, a user,
# or a point in time -- rows missing any of these are dropped rather than
# imputed, since there's no sensible default for an identity column.
REQUIRED_COLUMNS: List[str] = ["product_id", "user_id", "event_time", "event_type"]


def cast_schema(df: DataFrame) -> DataFrame:
    """Cast raw string columns to their target types.

    `event_time` arrives as e.g. '2019-10-01 00:00:00 UTC'. The trailing
    ' UTC' breaks a direct cast to timestamp, so it is stripped first.
    """
    return (
        df.withColumn(
            "event_time",
            F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC$", "")),
        )
        .withColumn("product_id", F.col("product_id").cast(LongType()))
        .withColumn("category_id", F.col("category_id").cast(LongType()))
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("user_id", F.col("user_id").cast(LongType()))
    )


def split_category_code(df: DataFrame) -> DataFrame:
    """Split the dotted `category_code` into up to 3 levels.

    `category_code` arrives as e.g. 'electronics.smartphone.samsung' with
    1-3 dot-separated segments. A missing deeper level resolves to NULL
    (not empty string), so downstream GROUP BY / COUNT doesn't silently
    lump "no subcategory" together with "subcategory is an empty string".
    """
    parts = F.split(F.col("category_code"), r"\.")
    return (
        df.withColumn("category_1", parts.getItem(0))
        .withColumn("category_2", parts.getItem(1))
        .withColumn(
            "category_3",
            F.when(F.size(parts) > 2, parts.getItem(2)).otherwise(
                F.lit(None).cast(StringType())
            ),
        )
    )


def handle_missing_values(df: DataFrame) -> DataFrame:
    """Drop rows missing an identity/time field; fill categorical gaps.

    `brand` and the category levels are filled with 'unknown' instead of
    dropped, since "brand not provided" is itself a meaningful signal for
    the brand-share and funnel dashboards downstream.
    """
    before = df.count()
    df = df.dropna(subset=REQUIRED_COLUMNS)
    dropped = before - df.count()
    if dropped:
        logger.warning("Dropped %d rows missing a required field", dropped)

    # Only fill columns that are actually present: this function is also
    # exercised in isolation in tests (and could run before
    # split_category_code in some future pipeline ordering), so it must not
    # assume category_1/2/3 already exist.
    fill_candidates = {
        "brand": "unknown",
        "category_1": "unknown",
        "category_2": "unknown",
        "category_3": "unknown",
    }
    fill_values = {col: v for col, v in fill_candidates.items() if col in df.columns}
    return df.fillna(fill_values) if fill_values else df


def deduplicate(df: DataFrame) -> DataFrame:
    """Drop exact duplicate events on the natural key.

    The raw Kaggle export contains repeated rows from source-side retries
    during collection.
    """
    before = df.count()
    df = df.dropDuplicates(DEDUP_KEY_COLUMNS)
    removed = before - df.count()
    if removed:
        logger.info("Removed %d duplicate events", removed)
    return df


def clean_and_transform(df: DataFrame) -> DataFrame:
    """Full pipeline: cast types -> split category_code -> handle nulls -> de-dup.

    Order matters: casting first means `handle_missing_values` sees real
    NULLs (not the string "null") wherever a source cast failed, and
    de-duplicating last means dropped/filled rows don't distort duplicate
    counts.
    """
    df = cast_schema(df)
    df = split_category_code(df)
    df = handle_missing_values(df)
    df = deduplicate(df)
    return df
