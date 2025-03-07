
import os
from time import sleep
import boto3
from botocore.errorfactory import ClientError
import os

__dynamodb_client = boto3.client('dynamodb')

__table_name_year_total = os.environ['YEAR_TOTAL']

def upsert_year_total(imo, year_and_ope, borrowing, data, timestamp, retry_count:int=42) -> None:

    while retry_count:
        try:
            _ = __dynamodb_client.put_item(
                TableName = __table_name_year_total,
                Item = {
                    # キー項目
                    "imo"          : {"S": imo},
                    "year_and_ope" : {"S": year_and_ope},

                    # バリュー項目
                    "distance"     : {"S": data["distance"]["S"]},
                    "total_lng"    : {"S": data["total_lng"]["S"]},
                    "total_hfo"    : {"S": data["total_hfo"]["S"]},
                    "total_lfo"    : {"S": data["total_lfo"]["S"]},
                    "total_mdo"    : {"S": data["total_mdo"]["S"]},
                    "total_mgo"    : {"S": data["total_mgo"]["S"]},
                    "total_foc"    : {"S": data["total_foc"]["S"]},
                    "eu_actual_foc": {"S": data["eu_actual_foc"]["S"]},
                    "eua"          : {"S": data["eua"]["S"]},
                    "cb"           : {"S": data["cb"]["S"]},
                    "banking"      : {"S": data["banking"]["S"]},
                    "borrowing"    : {"S": borrowing},
                    "fine_flag"    : {"S": data["fine_flag"]["S"]},
                    "timestamp"    : {"S": timestamp},
                },
                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
                # ConditionExpression="attribute_not_exists(#b)",
                # ExpressionAttributeNames={
                    # "#b": "b",
                # },
            )
            print(f"eco-euets-fueleu-year-total insert comleted. imo:{imo}, year_and_ope:{(year_and_ope)}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"eco-euets-fueleu-year-total insert. ClientError: code: {code}, Exception: {e}")

        print(f"eco-euets-fueleu-year-total upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)

