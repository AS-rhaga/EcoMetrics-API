#標準ライブラリ 
import os
from time import sleep
import boto3
from botocore.errorfactory import ClientError
import os

__dynamodb_client = boto3.client('dynamodb')

__table_name_simulation_voyage = os.environ['SIMULTATION_VOYAGE_PLAN']

def delete_simulation(imo):

    # imoをキーに削除を実施
    try:

        data = []
        response = __dynamodb_client.query(
            TableName=__table_name_simulation_voyage,
            ExpressionAttributeNames={
                '#name0': 'imo',
            },
            ExpressionAttributeValues={
                ':value0': {'S': imo}
            },
            KeyConditionExpression='#name0 = :value0'
        )

        print(f"response['Items']: {response['Items']}")

        # 取得したアイテムを削除
        for item in response['Items']:
            # 主キーとソートキーを指定して削除
            __dynamodb_client.delete_item(
                TableName=__table_name_simulation_voyage,
                Key={
                    'imo': item['imo'],
                    'year_and_serial_number': item['year_and_serial_number']
                }
            )

    except Exception as e:
        print('DynamoDBアイテム削除中に想定外のエラーが発生しました。imo:' + imo)
        print(e)
        return False

    return True