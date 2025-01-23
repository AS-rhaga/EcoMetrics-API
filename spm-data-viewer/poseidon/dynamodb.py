
from time import sleep
from botocore.errorfactory import ClientError
import boto3
import json
from datetime import datetime
import os

_dynamodb_client = boto3.client('dynamodb')

_table_name_user                    = os.environ['USER']
_table_name_group                   = os.environ['GROUP']
_table_name_noonreport              = os.environ['NOONREPORT']
_table_name_data_channel            = os.environ['DATA_CHANNEL']
_table_name_vesselmaster            = os.environ['VESSEL_MASTER']


# Select
# --------------------------------------------------------------------------------------------------------------------------------------
def get_user(user_id):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_user,
        ExpressionAttributeNames={
            '#name0': 'user_id',
        },
        ExpressionAttributeValues={
            ':value0': {'S': user_id}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data

def get_group(company_id):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_group,
        ExpressionAttributeNames={
            '#name0': 'company_id',
        },
        ExpressionAttributeValues={
            ':value0': {'S': company_id}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data
    
    
def get_noonreport(imo, timestamp_from, timestamp_to):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_noonreport,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'timestamp',
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': timestamp_from},
            ':value2': {'S': timestamp_to},
        },
        KeyConditionExpression='#name0 = :value0 AND #name1 Between :value1 and :value2'
    )
    data = response['Items']
    
    count = 0
    while 'LastEvaluatedKey' in response:
        response = _dynamodb_client.query(
            TableName=_table_name_noonreport,
            ExpressionAttributeNames={
                '#name0': 'imo_no',
                '#name1': 'timestamp',
                # '#name2': 'timestamp',
            },
            ExpressionAttributeValues={
                ':value0': {'S': imo},
                ':value1': {'S': timestamp_from},
                ':value2': {'S': timestamp_to},
            },
            KeyConditionExpression='#name0 = :value0 AND #name1 Between :value1 and :value2',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
        count += 1
    
    
    return data
    
    
def get_data_channel(imo):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_data_channel,
        ExpressionAttributeNames={
            '#name0': 'imo',
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data
    
def get_vesselmaster(imo):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_vesselmaster,
        ExpressionAttributeNames={
            '#name0': 'imo',
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data
    

# --------------------------------------------------------------------------------------------------------------------------------------


# Helper
# --------------------------------------------------------------------------------------------------------------------------------------
def timestream_formatter(timestamp):
    timestamp = timestamp.rstrip('Z').replace('T', '').replace(':', '').replace('-', '')
    date_from_year = timestamp[0:4]
    date_from_other = timestamp[4:15]
    date_from_lists = []
    for k in range(0,len(date_from_other), 2):
        date_from_lists.append(date_from_other[k:k+2])
    date_from = date_from_year + "-" + date_from_lists[0] + "-" + date_from_lists[1] + " " + date_from_lists[2] + ":" + date_from_lists[3] + ":" + date_from_lists[4]
    target_period_from    = datetime.fromisoformat(datetime.strptime(date_from, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'))
    timestamp = round(datetime.timestamp(target_period_from)*1000)
    timestamp = str(timestamp)
    
    return timestamp
# --------------------------------------------------------------------------------------------------------------------------------------