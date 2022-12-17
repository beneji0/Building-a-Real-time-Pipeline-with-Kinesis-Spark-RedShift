from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from configparser import ConfigParser
from pathlib import Path
import os

# Create a ConfigParser object
config = ConfigParser()

# Read the configuration file
path = Path(__file__)
ROOT_DIR = path.parent.absolute()
config_path = os.path.join(ROOT_DIR, "connections.ini")
config.read(config_path)

redshiftURL = config.get("REDSHIFT", "redshiftURL")
redshiftUser = config.get("REDSHIFT", "redshiftUser")
redshiftPassword = config.get("REDSHIFT", "redshiftPassword")
redshiftTable = config.get("REDSHIFT", "redshiftTable")

streamName = config.get("KINESIS", "streamName")
region = config.get("KINESIS", "region")
awsAccessKey = config.get("KINESIS", "awsAccessKey")
awsSecretKey = config.get("KINESIS", "awsSecretKey")

# Create a SparkSession object
spark = SparkSession.builder.appName("KinesisToRedshift").getOrCreate()

schema = StructType([
    StructField('id',StringType()),
    StructField('room_id' ,StringType()),
    StructField('noted_date' ,TimestampType()),
    StructField('temp',IntegerType()),
    StructField('out/in',StringType())]
)

# Create a DataFrame that represents the stream of data from Kinesis
kinesis_df = spark.readStream.format("kinesis") \
    .option("streamName", streamName) \
    .option("awsAccessKey", awsAccessKey) \
    .option("awsSecretKey", awsSecretKey) \
    .option("startingPosition", "latest") \
    .option("schema", schema)\
    .option("region", region) \
    .load()

# Replace empty fields with null values in all columns
transformed_df = kinesis_df.na.fill("") \
    .select([when(col(c).isin(""), None).otherwise(col(c)).alias(c) for c in kinesis_df.columns])

# Create an expression to check if all of the columns are "None"
all_none_expression = reduce(lambda x, y: x & y, [col(c) == "None" for c in transformed_df.columns])

# Remove rows that contain only the value "None"
transformed_df = transformed_df.where(~all_none_expression)

# Deduplicate the data by grouping by the unique identifier and selecting the first row
deduplicated_df = transformed_df.groupBy("id") \
    .agg(first("room_id").alias("room_id"), \
        first("noted_date").alias("noted_date"),\
        first("temp").alias("temp"),\
        first("out/in").alias("out/in"))

# Write the transformed data to Redshift
query = deduplicated_df.writeStream \
    .format("jdbc") \
    .option("url", redshiftURL) \
    .option("dbtable", redshiftTable) \
    .option("user", redshiftUser) \
    .option("password", redshiftPassword) \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

# Wait for the streaming query to finish
query.awaitTermination()
