
import os
from time import sleep
import boto3
from botocore.errorfactory import ClientError
import os

__dynamodb_client = boto3.client('dynamodb')

__table_name_year_total = os.environ['YEAR_TOTAL']

def get_year_total(imo, year_and_ope):

    data = []
    response = __dynamodb_client.query(
        TableName=__table_name_year_total,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'year_and_ope'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': year_and_ope}
        },
        KeyConditionExpression='#name0 = :value0 AND #name1 = :value1'
    )
    data = response['Items']
    
    count = 0
    while 'LastEvaluatedKey' in response:
        response = __dynamodb_client.query(
            TableName=__table_name_year_total,
            ExpressionAttributeNames={
                '#name0': 'imo',
                '#name1': 'year_and_ope'
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
