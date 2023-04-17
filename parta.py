"""Part A. Time analysis
"""
import sys, string
import os
import operator
import socket
import time
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Part A")\
        .getOrCreate()
    
    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[7])
            return True
        except:
            return False
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
    
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    #Calculating number of transactions per month
    blocks = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines = blocks.filter(good_line)
    lines = clean_lines.map(lambda l: (l.split(',')[11]))
    transactions = lines.map(lambda b: (time.strftime("%m-%y",time.gmtime(int(b))),1)) \
        .reduceByKey(operator.add)
    print(transactions.take(1000))
    
    #Calculating the average value of transactions per month
    average = clean_lines.map(lambda l: (l.split(',')[11], l.split(',')[7]))
    average = average.map(lambda b: (time.strftime("%m-%y",time.gmtime(int(b[0]))),int(b[1])))
    average = average.reduceByKey(operator.add)
    average = transactions.join(average)
    average = average.map(lambda x:(x[0],x[1][1]/x[1][0]))
    print(average.take(1000))
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'parta_' + date_time + '/numtransactions.txt')
    my_result_object.put(Body=json.dumps(transactions.take(1000)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'parta_' + date_time + '/averagetransactions.txt')
    my_result_object.put(Body=json.dumps(average.take(1000)))
                         
    spark.stop()
