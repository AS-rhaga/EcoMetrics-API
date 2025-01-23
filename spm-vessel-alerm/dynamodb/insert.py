from time import sleep
from botocore.errorfactory import ClientError
import boto3
import os


_dynamodb_client = boto3.client('dynamodb')
# _table_name_vessel_alerm = os.environ['VESSEL_ALERM']
_table_name_vessel_alerm = os.environ['VESSEL_ALARM']

def upsert(imo_no, dataSet, retry_count: int=42) -> None:

    while retry_count:
        try:
            # imo_no_fetch, timestamp_fetch, value_fetch = fetch_raw(imo_no, timestamp, dynamodb_client, table_name)
            # if row_dict_bin is None:
            # 
            # else:
            #     print(f"exist")
                
            _ = _dynamodb_client.put_item(
                TableName=_table_name_vessel_alerm,
                Item={
                    "imo"                       : {"S":  dataSet["imo"]},
                    "year"                      : {"S":  dataSet["year"]},
                    "cp"                        : {"S":  dataSet["cp"]},
                    "cp_from"                   : {"S":  dataSet["cp_from"]},
                    "cp_to"                     : {"S":  dataSet["cp_to"]},
                    "favorite"                  : {"S":  dataSet["favorite"]},
                    "Januarytonow_val"          : {"S":  dataSet["Januarytonow_val"]},
                    "Januarytonow"              : {"S":  dataSet["Januarytonow"]},
                    "Januarytonow_from"         : {"S":  dataSet["Januarytonow_from"]},
                    "Januarytonow_to"           : {"S":  dataSet["Januarytonow_to"]},
                    "Januarytonow_distance"     : {"S":  dataSet["Januarytonow_distance"]},
                    "Januarytonow_foc"          : {"S":  dataSet["Januarytonow_foc"]},
                    "Januarytonow_co2"          : {"S":  dataSet["Januarytonow_co2"]},
                    "LastYear_val"              : {"S":  dataSet["LastYear_val"]},
                    "LastYear"                  : {"S":  dataSet["LastYear"]},
                    "LastYear_from"             : {"S":  dataSet["LastYear_from"]},
                    "LastYear_to"               : {"S":  dataSet["LastYear_to"]},
                    "LastYear_distance"         : {"S":  dataSet["LastYear_distance"]},
                    "LastYear_foc"              : {"S":  dataSet["LastYear_foc"]},
                    "LastYear_co2"              : {"S":  dataSet["LastYear_co2"]},
                    "LatestUpdate"              : {"S":  dataSet["LatestUpdate"]},
                    "oneMonth_val"              : {"S":  dataSet["oneMonth_val"]},
                    "oneMonth"                  : {"S":  dataSet["oneMonth"]},
                    "oneMonth_from"             : {"S":  dataSet["oneMonth_from"]},
                    "oneMonth_to"               : {"S":  dataSet["oneMonth_to"]},
                    "oneMonth_distance"         : {"S":  dataSet["oneMonth_distance"]},
                    "oneMonth_foc"              : {"S":  dataSet["oneMonth_foc"]},
                    "oneMonth_co2"              : {"S":  dataSet["oneMonth_co2"]},
                    "oneMonth_count"            : {"S":  dataSet["oneMonth_count"]},
                    "rf"                        : {"S":  dataSet["rf"]},
                    "rf_from"                   : {"S":  dataSet["rf_from"]},
                    "rf_to"                     : {"S":  dataSet["rf_to"]},
                    # "VesselName"              : {"S":  dataSet["VesselName"]},
                },
                ReturnValues="NONE",
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
                ConditionExpression="attribute_not_exists(#b)",
                ExpressionAttributeNames={
                    "#b": "b",
                },
            )
            # print(f"insert comleted. imo_no: {imo_no}, dataSet: {dataSet}")
                
            return
        
        except ClientError as e:
            code = e.response["Error"]["Code"]
            print(f"ClientError: code: {code}, Exception: {e}")
            # if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            #     raise

        print(f"upsert cond check failed.")
        retry_count -= 1
        sleep(0.5)