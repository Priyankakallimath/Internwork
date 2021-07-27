import boto3
import json
import csv
from datetime import datetime
import calendar
import time
import random
import pandas as pd
import os
import json
import requests
from bson import json_util
import sys

# Reading CSV and saving as json file

class MetaClass(type):
    _instance ={}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instance:
            cls._instance[cls] = super(MetaClass, cls).__call__(*args,**kwargs)
            return cls._instance[cls]



class KinesisFireHose(metaclass=MetaClass):
    def __init__(self, StreamName=None):
        self.streamName=StreamName
        self.client = boto3.client('firehose', "us-east-1")
    @property
    def describe(self):
        response = self.client.describe_delivery_stream(DeliveryStreamName=self.streamName,Limit=123)
        response_json = json.dumps(response,indent=3,default=json_util.default)
        return response_json

    def post(self, payload=None):
        json_payload =json.dumps(payload)
        json_payload += "\n"
        json_payload_encode = json_payload.encode("utf-8")
        response = self.client.put_record(DeliveryStreamName=self.streamName, Record={'Data':json_payload_encode})
        response_aws = json.dumps(response, indent=3)
        return response_aws

def main():
    name = pd.read_json('/home/priyanka/airflow/de_sample_json.json')
    name = name.to_json()
    #print(name)
    kinesis_helper = KinesisFireHose(StreamName="kinesisexample")
    response = kinesis_helper.describe
    #print(response)
    response = kinesis_helper.post(payload=name)
    print(response)



if __name__=="__main__":
    main()