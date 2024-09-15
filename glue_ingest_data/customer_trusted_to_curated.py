import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Script generated for node Customer Trusted
CustomerTrustedByCustomer_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "multiline": False
    },
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedByCustomer_node",
)


# Script generated for node Accelerometer Landing
AccelerometerLandingByCustomer_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "multiline": False
    },
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLandingByCustomer_node",
)


# Script generated for node Customer mapping Accelerometer by User, Email
CustomerAccelerometerByCustomer_node = Join.apply(
    frame1=AccelerometerLandingByCustomer_node,
    frame2=CustomerTrustedByCustomer_node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerAccelerometerByCustomer_node",
)


# Script generated for node drop column in Accelerometer table
DropColumnByCustomer_node = DropFields.apply(
    frame=CustomerAccelerometerByCustomer_node,
    paths=[
        "user",
        "timeStamp",
        "x",
        "y",
        "z",
    ],
    transformation_ctx="DropColumnByCustomer_node",
)


# Script generated for node Customer Curated
CustomerCuratedByCustomer_node = glueContext.write_dynamic_frame.from_options(
    frame=DropColumnByCustomer_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCuratedByCustomer_node",
)


job.commit()
