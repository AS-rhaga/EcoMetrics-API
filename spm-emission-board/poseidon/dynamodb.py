
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
_table_name_cii_ref                 = os.environ['CII_REFERENCE']
_table_name_cii_rating              = os.environ['CII_RATING']
_table_name_cii_reduction_rate      = os.environ['CII_REDUCTION_RATE']
_table_name_vesselmaster            = os.environ['VESSEL_MASTER']
_table_name_fuel_oil_type           = os.environ['FUEL_OIL_TYPE']
#_table_name_cii_vesselalerm         = os.environ['VESSEL_ALERM']
_table_name_cii_vesselalerm         = os.environ['VESSEL_ALARM']


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
    
    
    
def get_cii_ref(ship_type):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_cii_ref,
        ExpressionAttributeNames={
            '#name0': 'type',
        },
        ExpressionAttributeValues={
            ':value0': {'S': ship_type}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data
    
def get_cii_rating(ship_type):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_cii_rating,
        ExpressionAttributeNames={
            '#name0': 'type',
        },
        ExpressionAttributeValues={
            ':value0': {'S': ship_type}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data
    
    
def get_cii_reduction_rate(year):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_cii_reduction_rate,
        ExpressionAttributeNames={
            '#name0': 'year',
        },
        ExpressionAttributeValues={
            ':value0': {'S': year}
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
    
    
    
    
def get_vesselalerm(imo, year):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_cii_vesselalerm,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'year'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': year}
        },
        KeyConditionExpression='#name0 = :value0 AND #name1 = :value1'
    )
    data = response['Items']
    
    return data
    
    
# --------------------------------------------------------------------------------------------------------------------------------------

