import sys
import boto3
import io
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from pyspark.sql import SQLContext
import deltaClass
from delta import *
import openpyxl

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

glueContext.setConf("spark.sql.broadcastTimeout", "20000000")

# Fetch current date
date_time = datetime.now().strftime("%Y-%m-%d")
now = datetime.now()
partition_filter = now - timedelta(days=45)
formatted_partition_days = partition_filter.strftime("%Y-%m-%d")

# ----------------------------Loading tables into Spark DataFrames-------------------------------
tbl_name = "vms_virtualaccount"
delta_path = f"delta_tablelocation"
vms_virtualaccount_df = spark.read.format("delta").load(delta_path)

tbl_name = "eb_merchant_info"
delta_path = f"delta_tablelocation"
eb_merchant_info_df = spark.read.format("delta").load(delta_path)

tbl_name = "vms_transferrequest"
delta_path = f"delta_tablelocation"
vms_transferrequest_df = spark.read.format("delta").load(delta_path).filter(col("_date") > lit(formatted_partition_days))

tbl_name = "vms_apirequest"
delta_path = f"delta_tablelocation"
vms_apirequest_df = spark.read.format("delta").load(delta_path).filter(col("_date") > lit(formatted_partition_days))

# --------------------------------Getting data from raw file--------------------------------------------
# Target bucket for unloading data
BUCKET_NAME = "bucket_path"
PREFIX = "Accounts_automation_reports/raw_folder"

client = boto3.client('s3')

# Fetch current date
date_time = datetime.now().strftime("%Y-%m-%d")

# Getting S3 response
response = client.list_objects(
    Bucket=BUCKET_NAME,
    Prefix=PREFIX,
)

# Returns the name of the output file
name = response["Contents"][-1]["Key"]
lastpick = name.split(sep="/")[-1]  # Picked the last value after /

# Reading the file from the S3 path
source_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"s3://{BUCKET_NAME}/{PREFIX}/{lastpick}").toPandas()

print('Input file is')
print(source_df.head())

# -----------------Deleting the source file----------------------------------------------------
s3_path_existing_file_check = f"s3://{BUCKET_NAME}/{PREFIX}/{lastpick}"

print('Last file of today is')
print(s3_path_existing_file_check)

try:
    # Delete the object if it exists
    client.delete_object(Bucket="kafka-inital-test-bucket", Key=s3_path_existing_file_check)
    print("Object deleted successfully from S3:", s3_path_existing_file_check)
    s3 = boto3.resource('s3')
    s3.Object(BUCKET_NAME, name).delete()
except client.exceptions.NoSuchKey as e:
    print("Object does not exist in S3:", s3_path_existing_file_check)

# -------------Iterate through input file and query with DataFrames------------------------------------

for index, row in source_df.iterrows():
    MID = row['MID']
    Start_Date = row['Start Date']
    End_Date = row['End Date']

    # Join DataFrames using Spark DataFrame API
    query_result = vms_apirequest_df \
        .join(vms_virtualaccount_df, vms_apirequest_df["virtual_account_id"] == vms_virtualaccount_df["id"], "left") \
        .join(eb_merchant_info_df, vms_apirequest_df["merchant_id"] == eb_merchant_info_df["id"], "left") \
        .select(
            eb_merchant_info_df["easebuzz_id"].alias("MID"),
            from_utc_timestamp(vms_apirequest_df["created_at"], "Asia/Kolkata").alias("Requested_Date"),
            vms_apirequest_df["merchant_request_number"].alias("Unique_Request_Number"),
            vms_apirequest_df["api_type"].alias("Utility_Type"),
            vms_apirequest_df["service_charge"],
            vms_apirequest_df["gst_amount"],
            vms_apirequest_df["service_charge_with_gst"],
            vms_apirequest_df["status"]
        ).filter(
            (eb_merchant_info_df["easebuzz_id"] == MID) &
            (vms_virtualaccount_df["is_connected_banking"] == "true") &
            (vms_apirequest_df["created_at"] > "2024-02-29 18:30:00") &
            (vms_apirequest_df["created_at"] <= "2024-03-31 18:30:00") &
            (vms_apirequest_df["is_valid"] == "true") &
            (vms_apirequest_df["status"] == "success")
        ).coalesce(1)

    total_row_count = query_result.count()
    print(total_row_count, 'Total row count is')

    df = query_result.toPandas()

    try:
        if total_row_count > 0:
            df = df.reset_index()
            df = df.rename(columns={'index': 'Row'})
            df['Row'] = df['Row'] + 1
            
            chunk_size = 1000000
            num_chunks = int(df.shape[0] / chunk_size) + 1
            print(num_chunks, 'Number of chunks is')

            s3 = boto3.client('s3')

            for i in range(num_chunks):
                start_idx = i * chunk_size
                end_idx = min(start_idx + chunk_size, df.shape[0])
                chunk = df.iloc[start_idx:end_idx]
                
                min_date_file = str(chunk['Requested_Date'].iloc[0])
                max_date_file = str(chunk['Requested_Date'].iloc[-1])
                
                if '.' in min_date_file:
                    dt_min = datetime.strptime(min_date_file, '%Y-%m-%d %H:%M:%S.%f')
                else:
                    dt_min = datetime.strptime(min_date_file, '%Y-%m-%d %H:%M:%S')
                
                formatted_min_date_file = dt_min.strftime('%Y_%m_%d')
                
                if '.' in max_date_file:
                    dt_max = datetime.strptime(max_date_file, '%Y-%m-%d %H:%M:%S.%f')
                else:
                    dt_max = datetime.strptime(max_date_file, '%Y-%m-%d %H:%M:%S')

                formatted_max_date_file = dt_max.strftime('%Y_%m_%d')

                file_name = f"Connectedbanking_rep_MID_{MID}_from_{formatted_min_date_file}_to_{formatted_max_date_file}_part{i+1}.xlsx"

                with pd.ExcelWriter(file_name) as writer:
                    chunk.to_excel(writer, sheet_name='data', index=False)

                s3.upload_file(file_name, 'kafka-inital-test-bucket', f'Accounts_automation_reports/Accounts_Operations_Reports/{date_time}/Connectedbanking_API_transaction/{file_name}')
    except Exception as e:
        print(f"An error occurred: {str(e)}")

job.commit()

