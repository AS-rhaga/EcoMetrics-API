
from botocore.errorfactory import ClientError
import boto3
from datetime import datetime
import os

_dynamodb_client = boto3.client('dynamodb')
_table_name_user               = os.environ['USER']
_table_name_group              = os.environ['GROUP']
_table_name_favorite           = os.environ['FAVORITE']
_table_name_vessel_master      = os.environ['VESSEL_MASTER']
_table_name_vessel_alarm       = os.environ['VESSEL_ALARM']
_table_name_cii_ref            = os.environ['CII_REFERENCE']
_table_name_cii_rating         = os.environ['CII_RATING']
_table_name_cii_reduction_rate = os.environ['CII_REDUCTION_RATE']
_table_name_year_total         = os.environ['YEAR_TOTAL']
_table_name_simulation_voyage  = os.environ['SIMULATION_VOYAGE']
_table_name_simulation_speed   = os.environ['SIMULATION_SPEED']
_table_name_fuel_oil_type      = os.environ['FUEL_OIL_TYPE']
_table_name_FOC_Foemulas       = os.environ['FOC_FORMULAS']
_table_name_simulation_voyage_cii   = os.environ['SIMULATION_VOYAGE_CII']
_table_name_simulation_speed_cii    = os.environ['SIMULATION_SPEED_CII']

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

def get_favorite(user_id):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_favorite,
        ExpressionAttributeNames={
            '#name0': 'user_id',
        },
        ExpressionAttributeValues={
            ':value0': {'S': user_id},
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data
    
    
def get_vessel_master(imo):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_vessel_master,
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

def get_vessel_alarm(imo, year):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_vessel_alarm,
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
        TableName = _table_name_cii_rating,
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
        TableName = _table_name_cii_reduction_rate,
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

def get_year_total(imo):
    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_year_total,
        ExpressionAttributeNames={
            '#name0': 'imo',
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo}
        },
        KeyConditionExpression='#name0 = :value0'  
    )

def get_year_total_by_year(imo, year):

    data = []
    response = _dynamodb_client.query(
        TableName=_table_name_year_total,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'year_and_ope'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': year}
        },
        KeyConditionExpression='#name0 = :value0 AND begins_with(#name1, :value1)'
    )
    data = response['Items']
    
    count = 0
    while 'LastEvaluatedKey' in response:
        response = _dynamodb_client.query(
            TableName=_table_name_year_total,
            ExpressionAttributeNames={
                '#name0': 'imo',
                '#name1': 'company_and_year'
            },
            ExpressionAttributeValues={
                ':value0': {'S': imo},
                ':value1': {'S': year_and_ope}
            },
            KeyConditionExpression='#name0 = :value0 AND #name1 = :value1',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
        count += 1

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


def get_simulation_voyage(imo, year):

    response = _dynamodb_client.query(
        TableName=_table_name_simulation_voyage,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'year_and_serial_number'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': year}
        },
        KeyConditionExpression='#name0 = :value0 AND begins_with(#name1, :value1)'
    )

    # データが存在するか確認
    if 'Items' in response:
        return response['Items']
    else:
        return None       

def get_simulation_speed(imo, year):

    response = _dynamodb_client.query(
        TableName=_table_name_simulation_speed,
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
    # データが存在するか確認
    if 'Items' in response:
        return response['Items']
    else:
        return None

def get_foc_formulas(imo):
    response = _dynamodb_client.query(
        TableName=_table_name_FOC_Foemulas,
        ExpressionAttributeNames={
            '#name0': 'imo'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    # データが存在するか確認
    if 'Items' in response:
        return response['Items']
    else:
        return None

def get_simulation_voyage_cii(imo, year):

    response = _dynamodb_client.query(
        TableName=_table_name_simulation_voyage_cii,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'year_and_serial_number'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': year}
        },
        KeyConditionExpression='#name0 = :value0 AND begins_with(#name1, :value1)'
    )

    # データが存在するか確認
    if 'Items' in response:
        return response['Items']
    else:
        return None       

def get_simulation_speed_cii(imo, year):

    response = _dynamodb_client.query(
        TableName=_table_name_simulation_speed_cii,
        ExpressionAttributeNames={
            '#name0': 'imo'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
        },
        KeyConditionExpression='#name0 = :value0'
    )
    # データが存在するか確認
    if 'Items' in response:
        return response['Items']
    else:
        return None