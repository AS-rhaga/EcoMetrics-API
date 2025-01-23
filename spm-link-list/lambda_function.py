import os
from Tools.reuse import S3ConfigTools, DynamoDBConfigTools
import pandas as pd
from typing import Tuple
import json

_TABLE_NAME = os.environ.get("TABLE_NAME")
    

def data_collect(file_key:str = "/tmp/file.xlsx") -> Tuple[dict,dict,str]:
    df = pd.read_excel(file_key, index_col=None, skiprows=1)
    
    id_dict = {}
    type_dict = {}
    
    for i in range(len(df)):
        company_app_name = df.columns[1]
        
        if type(df.iat[i,3]) != float:
            id_dict[df.iat[i,2]] = df.iat[i,3]
            type_dict[df.iat[i,2]] = df.iat[i,4]
            
    return id_dict, type_dict, company_app_name

def main(event):
    s3_tool = S3ConfigTools()
    bucket, key = s3_tool.s3_put_event_catch(event)
    s3_tool.s3_file_download(bucket, key,"xlsx")
    
    register_id, register_type, company_appname = data_collect()

    dynamo_tool = DynamoDBConfigTools(_TABLE_NAME)
    
    if company_appname == "SPAS":
        company_appname = "spm_spas"
    
    register_all = {
        "c": company_appname,
        "dataid": json.dumps(register_id),
        "datatype": json.dumps(register_type)
    }
    
    dynamo_tool.dynamo_put_item(register_all)

def lambda_handler(event,context):
    try:
        main(event)
    except Exception as e:
        print(e)
    
    