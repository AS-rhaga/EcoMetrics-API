
#標準ライブラリ 
import os
from time import sleep
import boto3
from botocore.errorfactory import ClientError
import os

__dynamodb_client = boto3.client('dynamodb')

__table_name_pooling_table = os.environ['POOLING_TABLE']

def delete_pooling_table(company_and_year):

    # company_and_yearをキーに削除を実施
    try:
        data = []
        response = __dynamodb_client.query(
            TableName=__table_name_pooling_table,
            ExpressionAttributeNames={
                '#name0': 'company_and_year',
            },
            ExpressionAttributeValues={
                ':value0': {'S': company_and_year}
            },
            KeyConditionExpression='#name0 = :value0'
        )

        print(f"response['Items']: {response['Items']}")

        # 取得したアイテムを削除
        for item in response['Items']:
            # 主キーとソートキーを指定して削除
            __dynamodb_client.delete_item(
                TableName=__table_name_pooling_table,
                Key={
                    'company_and_year': item['company_and_year'],
                    'group_name': item['group_name']
                }
            )

    except Exception as e:
        print('DynamoDBアイテム削除中に想定外のエラーが発生しました。company_and_year:' + company_and_year)
        print(e)
        return False

    return True