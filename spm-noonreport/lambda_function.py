
from Tools.reuse import S3ConfigTools, DynamoDBConfigTools
import requests
import os
import json
from datetime import datetime
import boto3
import traceback

dynamoconfig = DynamoDBConfigTools("spm-link-list")

_APP_KEY = os.environ.get("APP_KEY")

_QUERY_URL = os.environ.get("QUERY_URL")

_HEADERS = {"x-app-key":_APP_KEY, "Accept-Encoding":"gzip"}

def type_match(appdb_item: str, value_type: str) -> bool:
    try:
        if value_type == "string":
            str(appdb_item)
        elif value_type == "float":
            float(appdb_item)
        else:
            raise ValueError
    except Exception:
        return False
    
    return True

def main(imo: str, timestamp: str, company_db: str, event_type: str) -> None:
    
    key = {
        "c": company_db
    }
    
    response = dynamoconfig.dynamo_get_item(key)
    
    if "Item" in response:
        response = response["Item"]
    else:
        print("No Item")
        return
    
    param = {
        "offset": timestamp
    }

    url = _QUERY_URL.format(imo)
    
    try:
        req = requests.get(url, params=param, headers=_HEADERS)
        
    except Exception as e:
        print(f"requests.get e: {str(e)}")
        print(traceback.format_exc())
        raise e
        
    if req.headers.get("Content-Encoding"):
        print(f"req.text: {req.text}")
        return
    else:
        res_json = json.loads(req.text)

    keys = res_json["Package"]["TimeSeriesData"][0]["TabularData"][0]["DataChannelID"]
    data = res_json["Package"]["TimeSeriesData"][0]["TabularData"][0]["DataSet"][0]["Value"]
    
    appdb_items = dict(zip(keys,data))
    
    dataid_dict = json.loads(response["dataid"])
    datatype_dict = json.loads(response["datatype"])
    
    spm_id_list = list(dataid_dict)
    spm_type_list = list(datatype_dict.values())
    
    dynamoregistertable = DynamoDBConfigTools("eco-noonreport")
    
    if event_type == "INSERT" or event_type == "UPDATE" or event_type == "MODIFY":
        keys = {}
    
        for dataid in spm_id_list:
            spm_item_name = dataid_dict[dataid]
            appdb_data = appdb_items[dataid]
            value_type = datatype_dict[dataid]
            
            if type_match(appdb_data,value_type):
                if spm_item_name == "timestamp":
                    keys[spm_item_name] = datetime.strptime(appdb_data, "%Y/%m/%d %H:%M").strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    keys[spm_item_name] = appdb_data
                
        dynamoregistertable.dynamo_put_item(keys)
        
    elif event_type == "DELETE":
        pass
        
def lambda_handler(event,context):
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    try:
        imo = message["shipId"]
        timestamp = message["timeStamp"]
        company_db = message["appName"]
        event_type = message["eventName"]
        print("imo: {}, appdb: {}, timestamp: {}, event_type: {}".format(imo,company_db,timestamp, event_type))
        main(imo,timestamp,company_db, event_type)

        lambda_function_name1 = "spm-euets-fueleu-leg-total"
        payload = {
            "imo" : imo,
            "timestamp" : timestamp
        }

        try:
            client = boto3.client('lambda')
            client.invoke(
                FunctionName = lambda_function_name1,
                InvocationType = 'Event',
                LogType = 'Tail',
                Payload = json.dumps(payload)
            )
            print(f"message{type(payload)}: {payload} is sent.")
        except Exception as e:
            print(f"Couldn't invoke function : {lambda_function_name1}")
            print(json.dumps(str(e)))

    except Exception as e:
        print(f"e: {str(e)}")
        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }
    