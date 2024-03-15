import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1710043172665 = glueContext.create_dynamic_frame.from_catalog(
    database="proyecto1",
    table_name="no",
    transformation_ctx="AWSGlueDataCatalog_node1710043172665",
)

# Script generated for node Amazon S3
AmazonS3_node1710043176012 = glueContext.getSink(
    path="s3://eiglesiasrtrabajo1/trusted/no/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1710043176012",
)
AmazonS3_node1710043176012.setCatalogInfo(
    catalogDatabase="proyecto1", catalogTableName="no_trusted"
)
AmazonS3_node1710043176012.setFormat("glueparquet", compression="snappy")
AmazonS3_node1710043176012.writeFrame(AWSGlueDataCatalog_node1710043172665)
job.commit()
