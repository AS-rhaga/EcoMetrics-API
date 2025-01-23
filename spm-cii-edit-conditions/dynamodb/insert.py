#標準ライブラリ 
import os
from time import sleep
import boto3
from botocore.errorfactory import ClientError
import os

__dynamodb_client = boto3.client('dynamodb')

__table_name_simulation_voyage = os.environ['SIMULTATION_VOYAGE_PLAN']

def upsert_simulation(imo, data, retry_count:int=42) -> None:

    while retry_count:
        try:
            _ = __dynamodb_client.put_item(
                TableName = __table_name_simulation_voyage,
                Item = {
                    # キー項目
                    "imo"                      : {"S": imo},
                    "year_and_serial_number"   : {"S": data["year_and_serial_number"]},
                    
                    # バリュー項目
                    "departure_port"   : {"S": data["departure_port"]},
                    "departure_time"   : {"S": data["departure_time"]},
                    "arrival_port"     : {"S": data["arrival_port"]},
                    "arrival_time"     : {"S": data["arrival_time"]},
                    "distance"         : {"S": data["distance"]},
                    "dispracement"     : {"S": data["dispracement"]},
                    "fuel"             : {"S": data["fuel"]},
                    "flag"             : {"S": "0"},
                },
                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
                # ConditionExpression="attribute_not_exists(#b)",
                # ExpressionAttributeNames={
                    # "#b": "b",
                # },
            )
            print(f"eco-cii-simulation-cond-voyage-plan insert comleted. imo: {data["imo"]}, data: {data}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"eco-cii-simulation-cond-voyage-plan insert. ClientError: code: {code}, Exception: {e}")

        print(f"eco-cii-simulation-cond-voyage-plan upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)