
from time import sleep
from botocore.errorfactory import ClientError
import boto3
import os

_dynamodb_client = boto3.client('dynamodb')
_table_name_noonreport    = os.environ['NOONREPORT']
_table_name_leg_total     = os.environ['LEG_TOTAL']
_table_name_fuel_oil_type = os.environ['FUEL_OIL_TYPE']
_table_eco_lo_code_master = os.environ['LO_CODE_MASTER']

def get_fuel_oil_type(fuel_oil_type):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_fuel_oil_type,
        ExpressionAttributeNames={
            '#name0': 'fuel_oil_type',
        },
        ExpressionAttributeValues={
            ':value0': {'S': fuel_oil_type},
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
                '#name0': 'imo',
                '#name1': 'timestamp',
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

def get_leg_total(imo, year):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_leg_total,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'leg_no'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': year}
            
        },
        KeyConditionExpression = '#name0 = :value0 AND begins_with(#name1, :value1)'
    )
    data = response['Items']

    count = 0
    while 'LastEvaluatedKey' in response:
        response = _dynamodb_client.query(
            TableName=_table_name_leg_total,
            ExpressionAttributeNames={
                '#name0': 'imo',
                '#name1': 'leg_no'
            },
            ExpressionAttributeValues={
                ':value0': {'S': imo},
                ':value1': {'S': year}
            },
            KeyConditionExpression='#name0 = :value0 AND begins_with(#name1, :value1)',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
        count += 1

    return data

def get_port_record(port_code):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_eco_lo_code_master,
        ExpressionAttributeNames={
            '#name0': 'lo_code'
        },
        ExpressionAttributeValues={
            ':value0': {'S': port_code},
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data