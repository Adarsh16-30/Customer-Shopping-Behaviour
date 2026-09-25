"""
Unit tests for src/transformations.py.

Runs against a local SparkSession -- no AWS credentials, no Glue
environment, no `awsglue` package required. This is exactly why the
transform logic lives in a plain PySpark module instead of only inside the
Glue job script: it makes the pipeline's actual business logic testable in
CI, not just runnable in production.
"""
import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.transformations import (
    cast_schema,
    clean_and_transform,
    deduplicate,
    handle_missing_values,
    split_category_code,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("transformations-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


# All-string schema, matching how these columns actually arrive from the
# Glue Catalog / raw CSV before cast_schema() runs. Passed explicitly rather
# than relying on schema inference, since inference can't determine a type
# for a column that is NULL in every row of a small test batch.
RAW_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True),
    ]
)

RAW_ROW = {
    "event_time": "2019-11-17 10:15:00 UTC",
    "event_type": "purchase",
    "product_id": "1002544",
    "category_id": "2232732093077520756",
    "category_code": "electronics.smartphone.samsung",
    "brand": "samsung",
    "price": "489.99",
    "user_id": "512475445",
    "user_session": "abc-123",
}


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)


def test_cast_schema_strips_utc_and_parses_timestamp(spark):
    result = cast_schema(make_df(spark, [RAW_ROW])).collect()[0]

    assert result["event_time"] == datetime.datetime(2019, 11, 17, 10, 15, 0)
    assert result["product_id"] == 1002544
    assert result["price"] == 489.99


def test_split_category_code_three_levels(spark):
    result = split_category_code(make_df(spark, [RAW_ROW])).collect()[0]

    assert result["category_1"] == "electronics"
    assert result["category_2"] == "smartphone"
    assert result["category_3"] == "samsung"


def test_split_category_code_missing_third_level_is_null(spark):
    row = dict(RAW_ROW, category_code="electronics.smartphone")
    result = split_category_code(make_df(spark, [row])).collect()[0]

    assert result["category_3"] is None


def test_handle_missing_values_drops_rows_missing_required_fields(spark):
    good = dict(RAW_ROW)
    bad = dict(RAW_ROW, product_id=None)
    result = handle_missing_values(make_df(spark, [good, bad]))

    assert result.count() == 1


def test_handle_missing_values_fills_brand_gap(spark):
    row = dict(RAW_ROW, brand=None)
    result = handle_missing_values(make_df(spark, [row])).collect()[0]

    assert result["brand"] == "unknown"


def test_deduplicate_removes_exact_repeat_events(spark):
    result = deduplicate(make_df(spark, [RAW_ROW, RAW_ROW]))

    assert result.count() == 1


def test_clean_and_transform_end_to_end(spark):
    result = clean_and_transform(make_df(spark, [RAW_ROW, RAW_ROW]))

    assert result.count() == 1
    row = result.collect()[0]
    assert row["category_1"] == "electronics"
    assert row["product_id"] == 1002544
