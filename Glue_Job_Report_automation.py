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
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import numpy as np
import re
import pandas as pd
from datetime import datetime,timedelta
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

# spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
# spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#SparkContext.setSystemProperty('spark.sql.autoBroadcastJoinThreshold', '-1')
glueContext.setConf("spark.sql.broadcastTimeout", "20000000")

#fetch current date
date_time = datetime.now().strftime("%Y-%m-%d")
now = datetime.now()
partition_filter = now - timedelta(days=45) # partition filter for reading data
formated_partition_days = partition_filter.strftime("%Y-%m-%d")

#----------------------------Loading tables---------------------------------------------------------------
tbl_name = "vms_virtualaccount"
delta_path = f"delta_tablelocation"
vl_df = spark.read.format("delta").load(delta_path)
vl_df.createOrReplaceTempView(f"{tbl_name}")

tbl_name = "eb_merchant_info"
delta_path = f"delta_tablelocation"
vl_df = spark.read.format("delta").load(delta_path)
vl_df.createOrReplaceTempView(f"{tbl_name}")

tbl_name = "vms_transferrequest"
delta_path = f"delta_tablelocation"
vl_df = spark.read.format("delta").load(delta_path).filter(f"_date > date'{formated_partition_days}'")
vl_df.createOrReplaceTempView(f"{tbl_name}")

tbl_name = "eb_merchant_info"
delta_path = f"delta_tablelocation"
vl_df = spark.read.format("delta").load(delta_path)
vl_df.createOrReplaceTempView("public_eb_merchant_info")

tbl_name = "vms_apirequest"
delta_path = f"delta_tablelocation"
vl_df = spark.read.format("delta").load(delta_path).filter(f"_date > date'{formated_partition_days}'")
vl_df.createOrReplaceTempView("vms_apirequest")

#--------------------------------Getting data from raw file--------------------------------------------
#Target bucket for unloading data
BUCKET_NAME = "bucket_path"
PREFIX = "Accounts_automation_reports/raw_folder"

client = boto3.client('s3')

#fetch current date
date_time = datetime.now().strftime("%Y-%m-%d")

#getting S3 response
response = client.list_objects(
    Bucket=BUCKET_NAME,
    Prefix=PREFIX,
)

#returns the name of the output file
name = response["Contents"][-1]["Key"]
lastpick=name.split(sep="/")[-1]  #picked the last value after /

#reading the file from the S3 path
sourceDf = spark.read.option("header","true").option("delimiter", ",").csv("s3://kafka-inital-test-bucket/Accounts_automation_reports/raw_folder/"+lastpick).toPandas()

print('input file is')
print(sourceDf.head())

#-----------------deleting the source file----------------------------------------------------
s3_path_existing_file_check = "s3://Accounts_automation_reports/raw_folder/"+lastpick

print('last file of today is ')
print(s3_path_existing_file_check)

try:
    # Delete the object if it exists
    client.delete_object(Bucket="kafka-inital-test-bucket", Key=s3_path_existing_file_check)
    print("Object deleted successfully from S3:", s3_path_existing_file_check)
    s3 = boto3.resource('s3')
    s3.Object(BUCKET_NAME, name).delete()
except client.exceptions.NoSuchKey as e:
    print("Object does not exist in S3:", s3_path_existing_file_check)

#-------------------------------optional--------------------------------------------------------------------------

#-------------Iterate thorugh input file and query----------------------------------------------------------

for index, row in sourceDf.iterrows():
    #print(index,row)
    # get values from row
    MID = row['MID']
    Start_Date  = row['Start Date']
    End_Date = row['End Date']

    query_result = spark.sql(f"""
    select
    emi.easebuzz_id as MID,
    from_utc_timestamp(va.created_at, 'Asia/Kolkata')AS Requested_Date,
    va.merchant_request_number as Unique_Request_Number,
    va.api_type as Utility_Type,
    va.service_charge ,
    va.gst_amount ,
    va.service_charge_with_gst,
    va.status
    from
        vms_apirequest va
    left join vms_virtualaccount vv on
        va.virtual_account_id = vv.id
    left outer join public_eb_merchant_info emi on va.merchant_id = emi.id 
    where 
    emi.easebuzz_id = {MID}
    and 
    vv.is_connected_banking = 'true'
    and va.created_at >'2024-02-29 18:30:00' 
    and va.created_at <='2024-03-31 18:30:00' 
    and va.is_valid = 'true' 
    and va.status = 'success'
            """).coalesce(1)

    total_row_count = query_result.count()
    print(total_row_count,'total_row_count is')
    df = query_result.toPandas()
    
    try:
        if total_row_count>0:
            df = df.reset_index()
            # rename the new column to "Row"
            df = df.rename(columns={'index': 'Row'})
            # row count start
            df['Row'] = df['Row'] + 1
            
            chunk_size = 1000000 #define chunk size
            
            num_chunks = int(df.shape[0] / chunk_size) + 1
            print(num_chunks,'num of chunks is')
        
            s3 = boto3.client('s3')
        
            # split the DataFrame into chunks and write each chunk to a separate Excel file
            for i in range(num_chunks):
                start_idx = i * chunk_size
                #end_idx = min(start_idx + chunk_size, df.shape[0])
                if (start_idx + chunk_size)<df.shape[0]:
                    end_idx =start_idx + chunk_size
                else:
                    end_idx = df.shape[0]
                chunk = df.iloc[start_idx:end_idx]
                
                #take last and fist cell of column transfer date and remove white spaces and special char
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
                
                # formatted_min_date_file = min_date_file[:min_date_file.index(' ')] #re.sub('[^0-9]+', '', min_date_file.replace(' ', ''))
                # formatted_max_date_file = max_date_file[:max_date_file.index(' ')]#re.sub('[^0-9]+', '', max_date_file.replace(' ', ''))
                
                file_name = f"Connectedbanking_rep_MID_{MID}_from_{formatted_min_date_file}_to_{formatted_max_date_file}_part{i+1}.xlsx"
                #.format(MID, min_date_file, max_date_file)
    
                with pd.ExcelWriter(file_name) as writer:
                    chunk.to_excel(writer, sheet_name='data', index=False)
                s3.upload_file(file_name, 'kafka-inital-test-bucket', f'Accounts_automation_reports/Accounts_Operations_Reports/{date_time}/Connectedbanking_API_transaction/{file_name}')
    except Exception as e:
        print(f"An error occurred: {str(e)}")    

job.commit()