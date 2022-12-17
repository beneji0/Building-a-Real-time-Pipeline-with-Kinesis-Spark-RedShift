import boto3
import csv
import json
import time
from configparser import ConfigParser

config = ConfigParser()
config.read("connections.ini")

awsaccesskey = config.get("AWS", "awsAccessKey")
awssecretaccesskey = config.get("AWS", "awsSecretKey")
awsregion = config.get("KINESIS", "region")
streamname = config.get("KINESIS", "streamName")

session = boto3.Session(
    aws_access_key_id = awsaccesskey,\
    aws_secret_access_key = awssecretaccesskey ,\
    region_name = awsregion
    )

client = session.client("kinesis")

with open("./IOT-temp.csv") as f:
    fdict = csv.DictReader(f, delimiter=",")
    for row in fdict:
        data = json.dumps(dict(row))
        client.put_record(StreamName=streamname, Data=data, PartitionKey="1")
        time.sleep(5)
        print(data)