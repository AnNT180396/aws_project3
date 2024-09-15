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


# Script generated for node Accelerometer Landing
AccelerometerLandingByAccelerometer_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "multiline": False
    },
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLandingByAccelerometer_node",
)


# Script generated for node Customer Trusted
CustomerTrustedByAccelerometer_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "multiline": False
    },
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedByAccelerometer_node",
)


# Script generated for node Customer mapping Accelerometer by User, Email
CustomerAccelerometerByAccelerometer_node = Join.apply(
    frame1=AccelerometerLandingByAccelerometer_node,
    frame2=CustomerTrustedByAccelerometer_node,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerAccelerometerByAccelerometer_node",
)


# Script generated for node drop column in Customer table
DropColumnByAccelerometer_node = DropFields.apply(
    frame=CustomerAccelerometerByAccelerometer_node,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropColumnByAccelerometer_node",
)


# Script generated for node Accelerometer Trusted
AccelerometerTrustedByAccelerometer_node = glueContext.write_dynamic_frame.from_options(
    frame=DropColumnByAccelerometer_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrustedByAccelerometer_node",
)


job.commit()
