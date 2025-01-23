import boto3
import os

__dynamodb_client = boto3.client('dynamodb')
__table_name_user                = os.environ['USER']
__table_name_group               = os.environ['GROUP']
__table_name_fuel_oil_type       = os.environ['FUEL_OIL_TYPE']
__table_name_vessel_master       = os.environ['VESSEL_MASTER']
__table_name_simulation_voyage   = os.environ['SIMULTATION_VOYAGE_PLAN']
__table_name_simulation_speed    = os.environ['SIMULTATION_SPEED_PLAN']
__table_name_foc_formulas        = os.environ['FOC_FORMULAS']
__table_name_leg_total           = os.environ['LEG_TOTAL']
__table_name_year_total          = os.environ['YEAR_TOTAL']

# DBAccess-------------------------------------------------------------------------------------
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

def get_group_auth(company_id):
    data = []
    response = __dynamodb_client.query(
        TableName=__table_name_group,
        ExpressionAttributeNames={
            '#name0': 'company_id'
        },
        ExpressionAttributeValues={
            ':value0': {'S': company_id}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    return data

def get_group(company_id, group_id):
    data = []
    response = __dynamodb_client.query(
        TableName=__table_name_group,
        ExpressionAttributeNames={
            '#name0': 'company_id',
            '#name1': 'group_id'
        },
        ExpressionAttributeValues={
            ':value0': {'S': company_id},
            ':value1': {'S': group_id}
        },
        KeyConditionExpression='#name0 = :value0 AND #name1 = :value1'
    )
    data = response['Items']
    
    return data

def get_fuel_oil_type(fuel_oil_type):
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

def get_vessel_master(imo):
    data = []
    response = __dynamodb_client.query(
        TableName=__table_name_vessel_master,
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

def get_simulation_voyage(imo, year):

    response = __dynamodb_client.query(
        TableName=__table_name_simulation_voyage,
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

    response = __dynamodb_client.query(
        TableName=__table_name_simulation_speed,
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

def get_leg_total(imo, year):
    data = []
    response = __dynamodb_client.query(
        TableName = __table_name_leg_total,
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
        response = __dynamodb_client.query(
            TableName = __table_name_leg_total,
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

def get_year_total(imo):
    
    data = []
    response = __dynamodb_client.query(
        TableName =__table_name_year_total,
        ExpressionAttributeNames={
            '#name0': 'imo'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    count = 0
    while 'LastEvaluatedKey' in response:
        response = __dynamodb_client.query(
            TableName = __table_name_year_total,
            ExpressionAttributeNames={
                '#name0': 'imo'
            },
            ExpressionAttributeValues={
                ':value0': {'S': imo}
            },
            KeyConditionExpression='#name0 = :value0 #name0 = :value0',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
        count += 1

    return data