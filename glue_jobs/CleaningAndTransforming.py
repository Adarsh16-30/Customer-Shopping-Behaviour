"""
AWS Glue ETL entry point for the Customer Shopping Behaviour pipeline.

Reads the raw clickstream table from the Glue Data Catalog, applies the
transformations in transformations.py (uploaded alongside this script via
Glue's `--extra-py-files`), and writes the result back to S3 as
date-partitioned Parquet, keeping the Data Catalog in sync.

All bucket/database/table names are job parameters rather than hardcoded
literals, so the same script runs unmodified across dev/stage/prod -- see
infra/main.tf for how those parameters are set.
"""
import logging
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from transformations import clean_and_transform

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REQUIRED_ARGS = [
    "JOB_NAME",
    "SOURCE_DATABASE",
    "SOURCE_TABLE",
    "TARGET_DATABASE",
    "TARGET_TABLE",
    "TARGET_S3_PATH",
]


def main() -> None:
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session  # noqa: F841 (kept for parity/debugging in Glue console)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger.info("Reading source table %s.%s", args["SOURCE_DATABASE"], args["SOURCE_TABLE"])
    source_dyf = glue_context.create_dynamic_frame.from_catalog(
        database=args["SOURCE_DATABASE"],
        table_name=args["SOURCE_TABLE"],
        transformation_ctx="source",
    )

    raw_df = source_dyf.toDF()
    row_count_in = raw_df.count()
    logger.info("Read %d raw rows", row_count_in)

    transformed_df = clean_and_transform(raw_df)
    transformed_df = transformed_df.withColumn("event_date", F.to_date("event_time"))

    row_count_out = transformed_df.count()
    logger.info(
        "Writing %d transformed rows (dropped/deduped %d)",
        row_count_out,
        row_count_in - row_count_out,
    )

    transformed_dyf = DynamicFrame.fromDF(transformed_df, glue_context, "transformed")

    sink = glue_context.getSink(
        path=args["TARGET_S3_PATH"],
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["event_date"],  # Parquet + date partitioning: Athena only scans
        enableUpdateCatalog=True,      # the partitions a query's WHERE clause touches,
        transformation_ctx="sink",      # instead of the full 15GB on every query.
    )
    sink.setCatalogInfo(catalogDatabase=args["TARGET_DATABASE"], catalogTableName=args["TARGET_TABLE"])
    sink.setFormat("glueparquet")
    sink.writeFrame(transformed_dyf)

    job.commit()


if __name__ == "__main__":
    main()
