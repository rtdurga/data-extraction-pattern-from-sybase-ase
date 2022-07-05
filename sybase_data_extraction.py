import pyodbc
import sys
import boto3
import base64
from botocore.exceptions import ClientError
import json
import logging
import pandas as pd
from io import StringIO
import pg8000 as pg
from io import StringIO
import hashlib
import base64
import argparse
from datetime import date
current_date = date.today()
parser = argparse.ArgumentParser(add_help=True)
parser.add_argument('-st', '--source_table', default='')
parser.add_argument('-ss', '--source_schema', default='')
parser.add_argument('-r', '--region', default='us-east-1')
parser.add_argument('--columns_to_mask', nargs='+',default=[''])
parser.add_argument('-bk', '--bucket_name', default='ingestion-bucket')
parser.add_argument('-e','--encrypt',default='n')
parser.add_argument('-s_secret','--source_secret')
args = parser.parse_args()
if args.source_schema == '':
    source_tablename = args.source_table
else:
    source_tablename = args.source_schema + '.' + args.source_table
pii_column_list = args.columns_to_mask
logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
bucket = 's3://'+args.bucket_name+'/raw-data/'
csv_buffer = StringIO()
def encode(text):
    btext = text.encode('utf-8')
    return base64.b64encode(btext)
def get_secret(secret_name):
    region_name = args.region
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager',region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return json.loads(secret)
source_secret = args.source_secret
auth_source = get_secret(source_secret)
userName = auth_source['username']
passWord = auth_source['password']
host=auth_source['host']
port=auth_source['port']
dbname = auth_source['dbname']
syb_connection_string = 'DRIVER={Devart ODBC Driver for ASE};Server='+host+';Port='+port+';Database='+dbname+';User ID='+userName+';Password='+passWord+';String Types=Unicode'
conn_syb = pyodbc.connect(syb_connection_string)
print('Connected to Sybase source...')
syb_cursor = conn_syb.cursor()
syb_sql = "select * from "+source_tablename
syb_df = pd.read_sql(syb_sql, conn_syb)
row_count = syb_df.shape[0]
if row_count <= 0:
    print("No incremental data available from %s, hence no file will be uploaded to S3" %(source_tablename))
    syb_cursor.close()
    conn_syb.close()
    print("Closed all cursors and connections, exiting now...")
    exit()
elif pii_column_list[0] != '' and row_count > 0 and args.encrypt.lower()=='y':
    for col in pii_column_list:
        syb_df[col] = syb_df[col].fillna(' ')#replace nulls in the column
        syb_df[col] = syb_df.apply(lambda x: encode(x[col]), axis=1)
        syb_df[col] = syb_df[col].astype(str)#Type cast the encrypted column to string. Default is binary which is not readable by Athena or the Glue job
else:
    print("Fetched incremental data from %s" %(source_tablename))
s3_url = bucket+str(current_date.year)+'/'+str(current_date.month)+'/'+str(current_date.day)+'/'+source_tablename.lower()+'.parquet.snappy'
syb_df.to_parquet(s3_url, compression='snappy')
syb_cursor.close()
conn_syb.close()

