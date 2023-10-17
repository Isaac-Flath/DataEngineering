import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node CustomerTrusted
CustomerTrusted_node1697391466513 = glueContext.create_dynamic_frame.from_catalog(
    database="dend-project3",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1697391466513",
)

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="dend-project3",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Privacy Join
CustomerPrivacyJoin_node1697391511547 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1697391466513,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="CustomerPrivacyJoin_node1697391511547",
)

# Script generated for node Drop Columns
SqlQuery0 = """
select
    user,
    timestamp,
    x,
    y,
    z
from myDataSource

"""
DropColumns_node1697401955141 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": CustomerPrivacyJoin_node1697391511547},
    transformation_ctx="DropColumns_node1697401955141",
)

# Script generated for node AcceleromoterTrusted
AcceleromoterTrusted_node1697391609671 = glueContext.getSink(
    path="s3://isaac-data-lakehouse/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AcceleromoterTrusted_node1697391609671",
)
AcceleromoterTrusted_node1697391609671.setCatalogInfo(
    catalogDatabase="dend-project3", catalogTableName="accelerometer_trusted"
)
AcceleromoterTrusted_node1697391609671.setFormat("json")
AcceleromoterTrusted_node1697391609671.writeFrame(DropColumns_node1697401955141)
job.commit()