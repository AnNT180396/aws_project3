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


# Script generated for node Customer Landing
CustomerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "multiline": False
    },
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node",
)


# Script generated for node Share With Research
CustomerLandingFilter_node = Filter.apply(
    frame=CustomerLanding_node,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="CustomerLandingFilter_node",
)


# Script generated for node Customer Trusted
CustomerTrusted_node = glueContext.write_dynamic_frame.from_options(
    frame=CustomerLandingFilter_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node",
)


job.commit()
