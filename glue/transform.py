import json, sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, input_file_name, to_date

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_bucket", "curated_bucket", "curated_prefix", "input_keys"])
sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

objects = json.loads(args["input_keys"])
paths = [f"s3://{args['source_bucket']}/{obj['key']}" for obj in objects]
if not paths: raise ValueError("No valid Parquet input paths supplied")

df = spark.read.parquet(*paths)
# Starter transformation: replace with your schema checks and business transformations.
result = (df.withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .withColumn("_ingestion_date", to_date("_ingested_at")))
out = f"s3://{args['curated_bucket']}/{args['curated_prefix'].strip('/')}/"
result.write.mode("append").partitionBy("_ingestion_date").parquet(out)
job.commit()
