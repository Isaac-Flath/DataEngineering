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

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="dend-project3",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource
where shareWithResearchAsOfDate is not null
"""
SQLQuery_node1697398624141 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": CustomerLanding_node1},
    transformation_ctx="SQLQuery_node1697398624141",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.getSink(
    path="s3://isaac-data-lakehouse/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node2",
)
S3bucket_node2.setCatalogInfo(
    catalogDatabase="dend-project3", catalogTableName="customer_trusted"
)
S3bucket_node2.setFormat("json")
S3bucket_node2.writeFrame(SQLQuery_node1697398624141)
job.commit()