import boto3
import time
#from exceptions import Exception
from awsglue.utils import getResolvedOptions
import sys
args = getResolvedOptions(sys.argv,['command','instance_id'])              
command = args['command']
instance_id = args['instance_id'].split(',')
status = 'InProgress'
ssm_client = boto3.client('ssm')
response = ssm_client.send_command(
            InstanceIds=instance_id,
            DocumentName="AWS-RunShellScript",
            Parameters={'commands': [command]}, )
command_id = response['Command']['CommandId']
while status== 'InProgress':
    time.sleep(30)#SSM command execution from AWS Glue job is asynchronous
    res = ssm_client.get_command_invocation(
          CommandId=command_id,
          InstanceId=instance_id[0],
        )
    status=res['Status']
if res['Status'] != 'Success':
    raise Exception("The script failed")
