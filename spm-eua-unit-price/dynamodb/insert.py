import os
from time import sleep
import boto3
from botocore.errorfactory import ClientError
import os

__dynamodb_client = boto3.client('dynamodb')
__table_name_foc_formulas = os.environ['GROUP']

def upsert_group(group_data, eua_price, retry_count:int=42) -> None:

    while retry_count:
        try:
            _ = __dynamodb_client.put_item(
                TableName = __table_name_foc_formulas,
                Item = {
                    # キー項目
                    "company_id" : {"S": group_data[0]["company_id"]["S"]},
                    "group_id" : {"S": group_data[0]["group_id"]["S"]},
                    
                    # バリュー項目
                    "admin"              : {"S": group_data[0]["admin"]["S"]},
                    "imo_list"                : {"S": group_data[0]["imo_list"]["S"]},
                    "eua_price"     : {"S": eua_price},
                },
                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
                # ConditionExpression="attribute_not_exists(#b)",
                # ExpressionAttributeNames={
                    # "#b": "b",
                # },
            )
            print(f"eco-group insert comleted. company_id: {group_data[0]["company_id"]["S"]}, group_id: {group_data[0]["group_id"]["S"]}, data: {group_data}, eua_price: {eua_price}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"eco-group insert. ClientError: code: {code}, Exception: {e}")

        print(f"eco-group upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)