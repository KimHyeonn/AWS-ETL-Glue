import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import datetime

# month day check
day = datetime.date.today().day
month = datetime.date.today().month
year = datetime.date.today().year

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node S3 bucket
node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": False, "separator": ","},
    connection_type="s3",
    format = 'csv',
    connection_options={"paths": ["s3://aws-glue-toretto01/input/attribution/{}/{}/{}/part-00000".format(str(year), str(month), str(day))], "recurse": True},
    transformation_ctx="node1",
)

# Script generated for node ApplyMapping
node2 = ApplyMapping.apply(
    frame=node1,
    mappings=[
        ("col0", "string", "partner", "string"),
        ("col1", "string", "campaign", "string"),
        ("col2", "string", "server_datetime", "timestamp"),
        ("col3", "string", "traker_id", "string"),
        ("col4", "string", "log_id", "string"),
        ("col5", "string", "attribution_type", "int"),
        ("col6", "string", "identity_adid", "string"),
    ],
    transformation_ctx="node2",
)

# Script generated for node S3 bucket
node3 = glueContext.write_dynamic_frame.from_options(
    frame=node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://aws-glue-toretto01/output/attribution/{}/{}/{}/".format(str(year), str(month), str(day)),
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="node3",
)


job.commit()