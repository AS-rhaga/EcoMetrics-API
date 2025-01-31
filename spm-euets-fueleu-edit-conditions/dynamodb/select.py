import boto3
import os

__dynamodb_client = boto3.client('dynamodb')
__table_name_simulation_voyage = os.environ['SIMULTATION_VOYAGE_PLAN']
__table_name_foc_formulas      = os.environ['FOC_FORMULAS']
__table_name_fuel_oil_type      = os.environ['FUEL_OIL_TYPE']
__table_name_user                = os.environ['USER']
__table_name_group               = os.environ['GROUP']

# imoをキーに紐づく全レコードを取得
def get_simulation_voyage_plan(imo):

    response = __dynamodb_client.query(
        TableName=__table_name_simulation_voyage,
        ExpressionAttributeNames={
            '#name0': 'imo'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data

# FOC Formulas取得
def get_foc_formulas(imo):

    response = __dynamodb_client.query(
        TableName=__table_name_foc_formulas,
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

def get_fuel_oil_type():
    data = []
    response = __dynamodb_client.scan(
        TableName=__table_name_fuel_oil_type
    )
    data = response['Items']
    return data

def get_fuel_oil_type_by_oiletype(fuel_oil_type):
    data = []
    response = __dynamodb_client.query(
        TableName=__table_name_fuel_oil_type,
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

def get_user(user_id):
    data = []
    response = __dynamodb_client.query(
        TableName=__table_name_user,
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
    response = __dynamodb_client.query(
        TableName=__table_name_group,
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