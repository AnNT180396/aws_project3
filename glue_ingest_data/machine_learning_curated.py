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


# Script generated for node StepTrainer Trusted
StepTrainerTrusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "multiline": False
    },
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node",
)


# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "multiline": False
    },
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-project/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node",
)


# Script generated for node StepTrainer_node mapping Accelerometer by sensorReadingTime, timeStamp
AccelerometerStepTrainer_node = Join.apply(
    frame1=AccelerometerTrusted_node,
    frame2=StepTrainerTrusted_node,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="AccelerometerStepTrainer_node",
)


# Script generated for node drop column in Customer table
DropColumnByMachineLearning_node = DropFields.apply(
    frame=AccelerometerStepTrainer_node,
    paths=[
        "user",
    ],
    transformation_ctx="DropColumnByMachineLearning_node",
)


# Script generated for node StepTrainer Trusted
MachineLearningCurated_node = glueContext.write_dynamic_frame.from_options(
    frame=DropColumnByMachineLearning_node,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-project/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node",
)


job.commit()
