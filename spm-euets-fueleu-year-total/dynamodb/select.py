from time import sleep
from botocore.errorfactory import ClientError
import boto3
import os

_dynamodb_client = boto3.client('dynamodb')
_table_name_noonreport    = os.environ['NOONREPORT']
_table_name_leg_total     = os.environ['LEG_TOTAL']
_table_name_vesselmaster  = os.environ['VESSELMASTER']
_table_name_fuel_oil_type = os.environ['FUEL_OIL_TYPE']
_table_name_year_total    = os.environ['YEAR_TOTAL']
_table_name_pooling_group = os.environ['POOLING_GROUP']

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

def get_year_total(imo, year_and_ope):

    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_year_total,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'year_and_ope',
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': year_and_ope}
        },
        KeyConditionExpression='#name0 = :value0 AND #name1 = :value1'
    )
    data = response['Items']

    return data

# ソートキーの西暦部分で前方一致検索
def get_leg_total(imo, leg_no):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_leg_total,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'leg_no'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': leg_no}
        },
        KeyConditionExpression = '#name0 = :value0 AND #name1 > :value1'
        # KeyConditionExpression = '#name0 = :value0'

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
                ':value1': {'S': leg_no}
            },
            KeyConditionExpression='#name0 = :value0 AND #name1 > :value1',
            # KeyConditionExpression='#name0 = :value0',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
        count += 1

    return data

def get_pooling_table(company_and_year):
    
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_pooling_group,
        ExpressionAttributeNames={
            '#name0': 'company_and_year'
        },
        ExpressionAttributeValues={
            ':value0': {'S': company_and_year}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    count = 0
    while 'LastEvaluatedKey' in response:
        response = _dynamodb_client.query(
            TableName=_table_name_pooling_group,
            ExpressionAttributeNames={
                '#name0': 'imo',
                '#name1': 'timestamp',
            },
            ExpressionAttributeValues={
                ':value0': {'S': company_and_year}
            },
            KeyConditionExpression='#name0 = :value0 AND #name1',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
        count += 1

    return data