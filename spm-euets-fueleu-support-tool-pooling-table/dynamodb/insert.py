
#標準ライブラリ 
import os
import sys
import json
import boto3
from botocore.errorfactory import ClientError
import os

_dynamodb_client          = boto3.client('dynamodb')
_table_name_pooling_table = os.environ['POOLING_TABLE']
_table_name_year_total    = os.environ['YEAR_TOTAL']

def upsert_pooling_table(data, retry_count:int=42) -> None:

    while retry_count:
        try:
            _ = _dynamodb_client.put_item(
                TableName = _table_name_pooling_table,
                Item = {
                    # キー項目
                    "company_and_year": {"S": data["company_and_year"]},
                    "group_name"      : {"S": data["group_name"]},
                    
                    # バリュー項目
                    "imo_list"        : {"S": json.dumps(data["imo_list"])}
                },
                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
                # ConditionExpression="attribute_not_exists(#b)",
                # ExpressionAttributeNames={
                    # "#b": "b",
                # },
            )
            print(f"eco-fueleu-pooling-table insert comleted. company_and_year: {data["company_and_year"]}, data: {data}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"eco-foc-formulas insert. ClientError: code: {code}, Exception: {e}")

        print(f"eco-fueleu-pooling-table upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)

def upsert_year_total(data, banking_cb, pooling_group, retry_count: int=42) -> None:

    while retry_count:
        try:
            _ = _dynamodb_client.put_item(
                TableName=_table_name_year_total,
                Item={
                    # キー項目
                    "imo"         : { "S": data["imo"]["S"] },
                    "year_and_ope": { "S": data["year_and_ope"]["S"] },
                    
                    # バリュー項目(borrowing以外)
                    "distance"    : { "S": data["distance"]["S"] },
                    "total_lng"   : { "S": data["total_lng"]["S"] },
                    "total_hfo"   : { "S": data["total_hfo"]["S"] },
                    "total_lfo"   : { "S": data["total_lfo"]["S"] },
                    "total_mdo"   : { "S": data["total_mdo"]["S"] },
                    "total_mgo"   : { "S": data["total_mgo"]["S"] },
                    "total_foc"   : { "S": data["total_foc"]["S"] },
                    "eua"         : { "S": data["eua"]["S"] },
                    "cb"          : { "S": data["cb"]["S"] },
                    "banking"     : { "S": str(banking_cb) },
                    "fine_flag"   : { "S": data["fine_flag"]["S"] },
                    "pooling_group": { "S": pooling_group },
                    "timestamp"   : { "S": data["timestamp"]["S"] }
                },

                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
                ConditionExpression="attribute_not_exists(#b)",
                ExpressionAttributeNames={
                    "#b": "b",
                },
            )
            print(f"insert comleted. imo: {data["imo"]}, data: {data}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"ClientError: code: {code}, Exception: {e}")

        print(f"upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)