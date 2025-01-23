#標準ライブラリ 
import os
from time import sleep
import boto3
from botocore.errorfactory import ClientError
import os

__dynamodb_client = boto3.client('dynamodb')

__table_name_foc_formulas = os.environ['FOC_FORMULAS']

def upsert_vesselmaster(data, retry_count:int=42) -> None:

    while retry_count:
        try:
            _ = __dynamodb_client.put_item(
                TableName = __table_name_foc_formulas,
                Item = {
                    # キー項目
                    "imo" : {"S": data["imo"]},
                    
                    # バリュー項目
                    "me_ballast"              : {"S": data["me_ballast"]},
                    "me_laden"                : {"S": data["me_laden"]},
                    "auxiliary_equipment"     : {"S": data["auxiliary_equipment"]},
                },
                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
                # ConditionExpression="attribute_not_exists(#b)",
                # ExpressionAttributeNames={
                    # "#b": "b",
                # },
            )
            print(f"eco-foc-formulas insert comleted. imo: {data["imo"]}, data: {data}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"eco-foc-formulas insert. ClientError: code: {code}, Exception: {e}")

        print(f"eco-foc-formulas upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)