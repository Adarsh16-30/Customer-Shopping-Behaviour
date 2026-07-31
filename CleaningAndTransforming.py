import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import pyspark.sql.functions as f

    df = dfc.select(list(dfc.keys())[0]).toDF()

    new_df = (
        df.withColumn("category_1", f.split(df["category_code"], "\.").getItem(0))
        .withColumn("category_2", f.split(df["category_code"], "\.").getItem(1))
        .withColumn(
            "category_3",
            f.when(
                (f.size(f.split(df["category_code"], "\.")) > 2),
                f.split(df["category_code"], "\.").getItem(2),
            ).otherwise(""),
        )
    )

    newcustomerdyc = DynamicFrame.fromDF(new_df, glueContext, "newcustomerdata")

    return DynamicFrameCollection({"CustomTransform0": newcustomerdyc}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3bucket-project
S3bucketproject_node1668022964314 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="bigdataprojectbucket",
    transformation_ctx="S3bucketproject_node1668022964314",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1668023128365 = ApplyMapping.apply(
    frame=S3bucketproject_node1668022964314,
    mappings=[
        ("event_time", "string", "event_time", "timestamp"),
        ("event_type", "string", "event_type", "string"),
        ("product_id", "long", "product_id", "long"),
        ("category_id", "long", "category_id", "long"),
        ("category_code", "string", "category_code", "string"),
        ("brand", "string", "brand", "string"),
        ("price", "double", "price", "double"),
        ("user_id", "long", "user_id", "long"),
        ("user_session", "string", "user_session", "string"),
        ("partition_0", "string", "partition_0", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1668023128365",
)

# Script generated for node Custom Transform
CustomTransform_node1668035233774 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {
            "ChangeSchemaApplyMapping_node1668023128365": ChangeSchemaApplyMapping_node1668023128365
        },
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1668036229856 = SelectFromCollection.apply(
    dfc=CustomTransform_node1668035233774,
    key=list(CustomTransform_node1668035233774.keys())[0],
    transformation_ctx="SelectFromCollection_node1668036229856",
)

# Script generated for node Amazon S3
AmazonS3_node1668025053523 = glueContext.getSink(
    path="s3://bigdataprojectbucket/etlprojectoutput/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1668025053523",
)
AmazonS3_node1668025053523.setCatalogInfo(
    catalogDatabase="project", catalogTableName="TransformTable"
)
AmazonS3_node1668025053523.setFormat("csv")
AmazonS3_node1668025053523.writeFrame(SelectFromCollection_node1668036229856)
job.commit()
