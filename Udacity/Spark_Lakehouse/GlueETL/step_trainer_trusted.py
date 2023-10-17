import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1697493662759 = glueContext.create_dynamic_frame.from_catalog(
    database="dend-project3",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1697493662759",
)

# Script generated for node Customer Curated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="dend-project3",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node Join
StepTrainerLanding_node1697493662759DF = StepTrainerLanding_node1697493662759.toDF()
CustomerCurated_node1DF = CustomerCurated_node1.toDF()
Join_node1697493704189 = DynamicFrame.fromDF(
    StepTrainerLanding_node1697493662759DF.join(
        CustomerCurated_node1DF,
        (
            StepTrainerLanding_node1697493662759DF["serialnumber"]
            == CustomerCurated_node1DF["serialnumber"]
        ),
        "leftsemi",
    ),
    glueContext,
    "Join_node1697493704189",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.getSink(
    path="s3://isaac-data-lakehouse/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node2",
)
S3bucket_node2.setCatalogInfo(
    catalogDatabase="dend-project3", catalogTableName="step_trainer_trusted"
)
S3bucket_node2.setFormat("json")
S3bucket_node2.writeFrame(Join_node1697493704189)
job.commit()