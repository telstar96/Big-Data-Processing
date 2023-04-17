"""Part D. Popular Scams
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
        .appName("Part D Popular Scams")\
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
    
    transactionlines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    transactionslines = transactionlines.filter(good_line)
    transactions = transactionslines.map(lambda l: (l.split(',')[6], float(l.split(',')[7])))
    scamslines = spark.sparkContext.textFile("s3a://" + s3_bucket + "/scams.csv")
    scams = scamslines.map(lambda l: (l.split(',')[6],(l.split(',')[0], l.split(',')[4])))
    
    # The most lucrative scams
    
    scamvalues = transactions.join(scams)
    scamvalues = scamvalues.map(lambda x: ((x[1][1][0], x[1][1][1]), float(x[1][0])))
    scamvalues = scamvalues.reduceByKey(operator.add)
    top10 = scamvalues.takeOrdered(10, key = lambda x: -x[1])
    
    for one in top10:
        print("{}-{}:{}".format(one[0][0],one[0][1],one[1]))
    
    # scams over time
    scamstime = scamslines.map(lambda l: (l.split(',')[6],l.split(',')[4]))
    transactionstime = transactionslines.map(lambda l: (l.split(',')[6], l.split(',')[11], int(l.split(',')[7])))
    transactionstime = transactionstime.map(lambda b: (b[0], (time.strftime("%m-%y",time.gmtime(int(b[1]))), b[2])))
    scamsovertime = transactionstime.join(scamstime)
    scamsovertime = scamsovertime.map(lambda x:("{}:{}".format(x[1][0][0],x[1][1]),x[1][0][1])) \
        .reduceByKey(operator.add)
    print(scamsovertime.take(1000))
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'partdscams_' + date_time + '/scamvalues.txt')
    my_result_object.put(Body=json.dumps(top10))
    my_result_object = my_bucket_resource.Object(s3_bucket,'partdscams_' + date_time + '/scamsovertime.txt')
    my_result_object.put(Body=json.dumps(scamsovertime.take(1000)))
    
    spark.stop()