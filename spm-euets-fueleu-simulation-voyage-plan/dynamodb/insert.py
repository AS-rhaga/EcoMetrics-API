#標準ライブラリ 
import os
from time import sleep
import boto3
from botocore.errorfactory import ClientError
import os

__dynamodb_client = boto3.client('dynamodb')

__table_name_simulation_speed = os.environ['SIMULTATION_SPEED_PLAN']
__table_name_simulation_voyage = os.environ['SIMULTATION_VOYAGE_PLAN']

# SpeedPlanのflagを"1"に更新
def upsert_simulation_speed(imo, year, retry_count:int=42) -> None:

    while retry_count:
        try:
            _ = __dynamodb_client.update_item(
                TableName = __table_name_simulation_speed,
                Key={
                    'imo' : {"S": imo},
                    'year': {"S": year}
                },
                UpdateExpression="SET #field = :value",
                ExpressionAttributeNames={
                    '#field': 'flag'  # 更新したい項目名
                },
                ExpressionAttributeValues={
                    ':value': {"S": "0"}  # 更新後の値
                },
            )
            print(f"eco-euets-simulation-cond-speed-plan insert comleted. imo: {imo}, year: {year}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"eco-euets-simulation-cond-speed-plan insert. ClientError: code: {code}, Exception: {e}")

        print(f"eco-euets-simulation-cond-speed-plan upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)

def upsert_simulation_voyage(imo, year_and_serial_number, retry_count:int=42) -> None:

    while retry_count:
        try:
            _ = __dynamodb_client.update_item(
                TableName = __table_name_simulation_voyage,         
                Key={
                    'imo': {"S": imo},
                    'year_and_serial_number': {"S": year_and_serial_number}
                },
                UpdateExpression="SET #field = :value",
                ExpressionAttributeNames={
                    '#field': 'flag'  # 更新したい項目名
                },
                ExpressionAttributeValues={
                    ':value': {"S": "1"}  # 更新後の値
                },
            )

            print(f"eco-euets-simulation-cond-voyage-plan insert comleted. imo: {imo}, year_and_serial_number: {year_and_serial_number}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"eco-euets-simulation-cond-voyage-plan insert. ClientError: code: {code}, Exception: {e}")

        print(f"eco-euets-simulation-cond-voyage-plan upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)