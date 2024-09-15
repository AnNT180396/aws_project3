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


# Script generated for node StepTrainer Landing
StepTrainerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "multiline": False
    },
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node",
)


# Script generated for node StepTrainer Landing
CustomerCurated_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "multiline": False
    },
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node",
)


# Script generated for node Customer mapping Accelerometer by User, Email
CustomerStepTrainer_node = Join.apply(
    frame1=StepTrainerLanding_node,
    frame2=CustomerCurated_node,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="CustomerStepTrainer_node",
)


# Script generated for node drop column in Customer table
DropColumnByStepTrainer_node = DropFields.apply(
    frame=CustomerStepTrainer_node,
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
    transformation_ctx="DropColumnByStepTrainer_node",
)


# Script generated for node StepTrainer Trusted
StepTrainerTrusted_node = glueContext.write_dynamic_frame.from_options(
    frame=DropColumnByStepTrainer_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node",
)


job.commit()
