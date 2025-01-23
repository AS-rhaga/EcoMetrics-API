
import boto3
import os

__dynamodb_client = boto3.client('dynamodb')
__table_name_user          = os.environ['USER']
__table_name_vessel_master = os.environ['VESSELMASTER']
__table_name_group         = os.environ['GROUP']
__table_name_pooling_table = os.environ['POOLING_TABLE']
__table_name_year_total    = os.environ['YEAR_TOTAL']
__table_name_fuel_oil_type = os.environ['FUEL_OIL_TYPE']

def get_fuel_oil_type():
    data = []
    response = __dynamodb_client.scan(
        TableName=__table_name_fuel_oil_type
    )
    data = response['Items']
    return data

def get_year_total_by_year(imo, year):
    data = []
    response = __dynamodb_client.query(
        TableName=__table_name_year_total,
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
        response = __dynamodb_client.query(
            TableName=__table_name_year_total,
            ExpressionAttributeNames={
                '#name0': 'imo',
                '#name1': 'company_and_year'
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
