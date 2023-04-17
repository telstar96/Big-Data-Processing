"""Part D. Gas Guzzlers
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
        .appName("Part D Gas Guzzlers")\
        .getOrCreate()
    
    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[8])
            float(fields[9])
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
    
    ## Number of transactions
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines = transactions.filter(good_line)
    numtransactions = clean_lines.map(lambda l: (l.split(',')[11]))
    numtransactions = numtransactions.map(lambda b: (time.strftime("%m-%y",time.gmtime(int(b))),1)) \
        .reduceByKey(operator.add)
    
    # Gas Price over time
    gas_price = clean_lines.map(lambda l: (l.split(',')[11], l.split(',')[9]))
    gas_price = gas_price.map(lambda b: (time.strftime("%m-%y",time.gmtime(int(b[0]))),int(b[1]))) \
        .reduceByKey(operator.add)
    gas_price = numtransactions.join(gas_price)
    gas_price = gas_price.map(lambda x:(x[0],x[1][1]/x[1][0]))
    print(gas_price.take(1000))
    
    ## Gas Usage over time
    gas_usage = clean_lines.map(lambda l: (l.split(',')[6], (l.split(',')[11], l.split(',')[8])))
    gas_usage = gas_usage.map(lambda b: (b[0],(time.strftime("%m-%y",time.gmtime(int(b[1][0]))),int(b[1][1]))))
    address = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    addressfeatures = address.map(lambda l: (l.split(',')[0], 1))
    gas_usage = gas_usage.join(addressfeatures)
    gas_usage = gas_usage.map(lambda x: (x[1][0][0], (x[1][1],x[1][0][1])))
    gas_usage = gas_usage.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
    gas_usage = gas_usage.map(lambda x: (x[0], x[1][1]/x[1][0]))
    print(gas_usage.take(1000))
    
    
    # Gas usage for the top 10 smart contracts
    features = clean_lines.map(lambda l: (l.split(',')[6], (float(l.split(',')[7]), float(l.split(',')[8]), time.strftime("%m-%y",time.gmtime(int(l.split(',')[11])))))) \
        .reduceByKey(operator.add)
    address = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")
    addressfeatures = address.map(lambda l: (l.split(',')[0], 1)) \
        .reduceByKey(operator.add)
    top10 = features.join(addressfeatures)
    top10 = top10.map(lambda x:("{}".format(x[0]),x[1][0][0],x[1][0][1],x[1][0][2]))
    top10 = top10.takeOrdered(10, key = lambda x: -x[1])
    
    for one in top10:
        print("{}: {};{};{}".format(one[0],one[1],one[2],one[3]))
    
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'partdgas_' + date_time + '/gasprice.txt')
    my_result_object.put(Body=json.dumps(gas_price.take(1000)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'partdgas_' + date_time + '/gasused.txt')
    my_result_object.put(Body=json.dumps(gas_usage.take(1000)))
    my_result_object = my_bucket_resource.Object(s3_bucket,'partdgas_' + date_time + '/top10gasused.txt')
    my_result_object.put(Body=json.dumps(top10))
                         
    spark.stop()