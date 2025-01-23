import boto3
import os

__dynamodb_client = boto3.client('dynamodb')
__table_name_user                = os.environ['USER']
__table_name_group               = os.environ['GROUP']

def get_group(company_id, group_id):
    data = []
    response = __dynamodb_client.query(
        TableName=__table_name_group,
        ExpressionAttributeNames={
            '#name0': 'company_id',
            '#name1': 'group_id',
        },
        ExpressionAttributeValues={
            ':value0': {'S': company_id},
            ':value1': {'S': group_id}
        },
        KeyConditionExpression='#name0 = :value0 AND #name1 = :value1 '
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