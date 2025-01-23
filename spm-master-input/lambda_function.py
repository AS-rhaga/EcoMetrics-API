import io
import cgi
import json
import boto3
import base64
import json
from io import StringIO
from datetime import datetime, timedelta
import re

import auth
import dynamodb

# vesselmaster
def util_vesselmaster(response):
    print(f"res_vesselmaster: {response}")
    
    rev = len(response)-1
    
    vesselmaster = {
        "Imo"               : response[rev]["imo"]["S"],
        "BuildDate"         : response[rev]["BuildDate"]["S"],
        "Deadweight"        : response[rev]["Deadweight"]["S"],
        "Grosstongue"       : response[rev]["Grosstongue"]["S"],
        "LastDockDate"      : response[rev]["LastDockDate"]["S"],
        "HullCleaningDate"  : response[rev]["HullCleaningDate"]["S"],
        "OilType"           : response[rev]["OilType"]["S"],
        "Owner"             : response[rev]["Owner"]["S"],
        "Size"              : response[rev]["Size"]["S"],
        "VesselName"        : response[rev]["VesselName"]["S"],
        "VesselType"        : response[rev]["VesselType"]["S"],
        "Ballast"           : response[rev]["Ballast"]["S"],
        "Laden"             : response[rev]["Laden"]["S"],
    }
    
    return vesselmaster
    
    
# datachannel;
def util_datachannel(response):
    print(f"res_datachannel: {response}")
    
    rev = len(response)-1
    
    datachannel = {
        # キー項目
        "imo": response[rev]["imo"]["S"],
        "rev": response[rev]["rev"]["S"],
        
        # # バリュー項目
        "imo":response[rev]["imo"]["S"],
        "rev":response[rev]["rev"]["S"],
        "lower_beaufort":response[rev]["lower_beaufort"]["S"],
        "lower_boiler_foc":response[rev]["lower_boiler_foc"]["S"],
        "lower_course":response[rev]["lower_course"]["S"],
        "lower_displacement":response[rev]["lower_displacement"]["S"],
        "lower_dwt":response[rev]["lower_dwt"]["S"],
        "lower_ge_foc":response[rev]["lower_ge_foc"]["S"],
        "lower_gt":response[rev]["lower_gt"]["S"],
        "lower_lat":response[rev]["lower_lat"]["S"],
        "lower_lng":response[rev]["lower_lng"]["S"],
        "lower_log_distance":response[rev]["lower_log_distance"]["S"],
        "lower_log_speed":response[rev]["lower_log_speed"]["S"],
        "lower_me_foc":response[rev]["lower_me_foc"]["S"],
        "lower_me_load":response[rev]["lower_me_load"]["S"],
        "lower_me_rpm":response[rev]["lower_me_rpm"]["S"],
        "lower_og_distance":response[rev]["lower_og_distance"]["S"],
        "lower_og_speed":response[rev]["lower_og_speed"]["S"],
        "lower_swell_direction":response[rev]["lower_swell_direction"]["S"],
        "lower_swell_height":response[rev]["lower_swell_height"]["S"],
        "lower_swell_period":response[rev]["lower_swell_period"]["S"],
        "lower_total_foc":response[rev]["lower_total_foc"]["S"],
        "lower_wave_direction":response[rev]["lower_wave_direction"]["S"],
        "lower_wave_height":response[rev]["lower_wave_height"]["S"],
        "lower_wave_period":response[rev]["lower_wave_period"]["S"],
        "lower_wind_direction":response[rev]["lower_wind_direction"]["S"],
        "lower_wind_speed":response[rev]["lower_wind_speed"]["S"],
        "upper_beaufort":response[rev]["upper_beaufort"]["S"],
        "upper_boiler_foc":response[rev]["upper_boiler_foc"]["S"],
        "upper_course":response[rev]["upper_course"]["S"],
        "upper_displacement":response[rev]["upper_displacement"]["S"],
        "upper_dwt":response[rev]["upper_dwt"]["S"],
        "upper_ge_foc":response[rev]["upper_ge_foc"]["S"],
        "upper_gt":response[rev]["upper_gt"]["S"],
        "upper_lat":response[rev]["upper_lat"]["S"],
        "upper_lng":response[rev]["upper_lng"]["S"],
        "upper_log_distance":response[rev]["upper_log_distance"]["S"],
        "upper_log_speed":response[rev]["upper_log_speed"]["S"],
        "upper_me_foc":response[rev]["upper_me_foc"]["S"],
        "upper_me_load":response[rev]["upper_me_load"]["S"],
        "upper_me_rpm":response[rev]["upper_me_rpm"]["S"],
        "upper_og_distance":response[rev]["upper_og_distance"]["S"],
        "upper_og_speed":response[rev]["upper_og_speed"]["S"],
        "upper_swell_direction":response[rev]["upper_swell_direction"]["S"],
        "upper_swell_height":response[rev]["upper_swell_height"]["S"],
        "upper_swell_period":response[rev]["upper_swell_period"]["S"],
        "upper_total_foc":response[rev]["upper_total_foc"]["S"],
        "upper_wave_direction":response[rev]["upper_wave_direction"]["S"],
        "upper_wave_height":response[rev]["upper_wave_height"]["S"],
        "upper_wave_period":response[rev]["upper_wave_period"]["S"],
        "upper_wind_direction":response[rev]["upper_wind_direction"]["S"],
        "upper_wind_speed":response[rev]["upper_wind_speed"]["S"],
    }
    
    return datachannel
    
    
# spcurve
def util_spcurve(response):
    print(f"res_spcurve: {response}")
    
    # 必須ではない項目の返却値制御
    if 'description' in response[2]:
        spcurve1_description = response[2]["description"]["S"]
    else:
        spcurve1_description = ""
    
    if 'description' in response[3]:
        spcurve2_description = response[3]["description"]["S"]
    else:
        spcurve2_description = ""

    if 'description' in response[4]:
        spcurve3_description = response[4]["description"]["S"]
    else:
        spcurve3_description = ""

    if 'description' in response[5]:
        spcurve4_description = response[5]["description"]["S"]
    else:
        spcurve4_description = ""

    # 返却値の設定
    cpcurve = {
        "name"      : response[0]["name"]["S"],
        "a"         : response[0]["a"]["S"],
        "alpha"     : response[0]["alpha"]["S"],
        "c"         : response[0]["C"]["S"],
        "display"   : response[0]["display"]["S"],
    }
    rfcurve = {
        "name"      : response[1]["name"]["S"],
        "a"         : response[1]["a"]["S"],
        "alpha"     : response[1]["alpha"]["S"],
        "c"         : response[1]["C"]["S"],
        "display"   : response[1]["display"]["S"],
    }
    spcurve1 = {
        "name"      : response[2]["name"]["S"],
        "a"         : response[2]["a"]["S"],
        "alpha"     : response[2]["alpha"]["S"],
        "c"         : response[2]["C"]["S"],
        "display"   : response[2]["display"]["S"],
        "description"   : spcurve1_description,
    }
    spcurve2 = {
        "name"      : response[3]["name"]["S"],
        "a"         : response[3]["a"]["S"],
        "alpha"     : response[3]["alpha"]["S"],
        "c"         : response[3]["C"]["S"],
        "display"   : response[3]["display"]["S"],
        "description"   : spcurve2_description,
        
    }
    spcurve3 = {
        "name"      : response[4]["name"]["S"],
        "a"         : response[4]["a"]["S"],
        "alpha"     : response[4]["alpha"]["S"],
        "c"         : response[4]["C"]["S"],
        "display"   : response[4]["display"]["S"],
        "description"   : spcurve3_description,
    }
    spcurve4 = {
        "name"      : response[5]["name"]["S"],
        "a"         : response[5]["a"]["S"],
        "alpha"     : response[5]["alpha"]["S"],
        "c"         : response[5]["C"]["S"],
        "display"   : response[5]["display"]["S"],
        "description"   : spcurve4_description,
    }
    
    return cpcurve, rfcurve, spcurve1, spcurve2, spcurve3, spcurve4
    

def lambda_handler(event, context):
    print(f"event: {event}")
    
    body = event['body']
    token = event['headers']['Authorization']
    
    # マルチパートデータの境界を解析
    boundary = re.search(r'------WebKitFormBoundary[\w\d]+', body).group()
    
    # 各パートを抽出
    parts = body.split(boundary)
    
    # フォームデータの辞書
    form_data = {}
    cpcurve = {}
    rfcurve = {}
    spcurve1 = {}
    spcurve2 = {}
    spcurve3 = {}
    spcurve4 = {}
    # 各パートを解析して値を取得
    for part in parts:
        if 'Content-Disposition' in part:
            name_match = re.search(r'name="([^"]+)"', part)
            if name_match:
                name = name_match.group(1)
                value = part.split('\r\n\r\n')[1].strip()
                form_data[name] = value
                
                if name == "cpcurve_name":
                    cpcurve["name"] = value
                elif name == "cpcurve_a":
                    cpcurve["a"] = value
                elif name == "cpcurve_alpha":
                    cpcurve["alpha"] = value
                elif name == "cpcurve_c":
                    cpcurve["c"] = value
                elif name == "cpcurve_display":
                    cpcurve["display"] = value
        
                elif name == "rfcurve_name":
                    rfcurve["name"] = value
                elif name == "rfcurve_a":
                    rfcurve["a"] = value
                elif name == "rfcurve_alpha":
                    rfcurve["alpha"] = value
                elif name == "rfcurve_c":
                    rfcurve["c"] = value
                elif name == "rfcurve_display":
                    rfcurve["display"] = value
        
                elif name == "spcurve1_name":
                    spcurve1["name"] = value
                elif name == "spcurve1_a":
                    spcurve1["a"] = value
                elif name == "spcurve1_alpha":
                    spcurve1["alpha"] = value
                elif name == "spcurve1_c":
                    spcurve1["c"] = value
                elif name == "spcurve1_display":
                    spcurve1["display"] = value
                elif name == "spcurve1_description":
                    spcurve1["description"] = value
        
                elif name == "spcurve2_name":
                    spcurve2["name"] = value
                elif name == "spcurve2_a":
                    spcurve2["a"] = value
                elif name == "spcurve2_alpha":
                    spcurve2["alpha"] = value
                elif name == "spcurve2_c":
                    spcurve2["c"] = value
                elif name == "spcurve2_display":
                    spcurve2["display"] = value
                elif name == "spcurve2_description":
                    spcurve2["description"] = value

        
                elif name == "spcurve3_name":
                    spcurve3["name"] = value
                elif name == "spcurve3_a":
                    spcurve3["a"] = value
                elif name == "spcurve3_alpha":
                    spcurve3["alpha"] = value
                elif name == "spcurve3_c":
                    spcurve3["c"] = value
                elif name == "spcurve3_display":
                    spcurve3["display"] = value
                elif name == "spcurve3_description":
                    spcurve3["description"] = value
        
                elif name == "spcurve4_name":
                    spcurve4["name"] = value
                elif name == "spcurve4_a":
                    spcurve4["a"] = value
                elif name == "spcurve4_alpha":
                    spcurve4["alpha"] = value
                elif name == "spcurve4_c":
                    spcurve4["c"] = value
                elif name == "spcurve4_display":
                    spcurve4["display"] = value
                elif name == "spcurve4_description":
                    spcurve4["description"] = value
                
    
    # 必要な値を取得
    imo = form_data.get('imo')
    get = form_data.get('get')
    print(f"imo[{type(imo)}]:, {imo}, get[{type(get)}]: {get}")
    
    # 認可：IMO参照権限チェック
    authCheck = auth.imo_check(token, imo)
    if authCheck == 401 or authCheck == 500:
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Headers" : "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },  
            'body': authCheck
        }
    
    cpcurve["imo"] = imo
    cpcurve["no"] = "1"
    rfcurve["imo"] = imo
    rfcurve["no"] = "2"
    spcurve1["imo"] = imo
    spcurve1["no"] = "3"
    spcurve2["imo"] = imo
    spcurve2["no"] = "4"
    spcurve3["imo"] = imo
    spcurve3["no"] = "5"
    spcurve4["imo"] = imo
    spcurve4["no"] = "6"
    
    # CPCurve、RFCurveのdescriptionに空文字を設定
    cpcurve["description"] = ""
    rfcurve["description"] = ""
    
    print(f"form_data: type: {type(form_data)}, value: {form_data}")
    print(f"cpcurve: type: {type(cpcurve)}, value: {cpcurve}")
    print(f"rfcurve: type: {type(rfcurve)}, value: {rfcurve}")
    print(f"spcurve1: type: {type(spcurve1)}, value: {spcurve1}")
    print(f"spcurve2: type: {type(spcurve2)}, value: {spcurve2}")
    print(f"spcurve3: type: {type(spcurve3)}, value: {spcurve3}")
    print(f"spcurve4: type: {type(spcurve4)}, value: {spcurve4}")
        
    if get == None:
        dynamodb.upsert_vesselmaster(form_data)
        dynamodb.upsert_datachannel(form_data)
        dynamodb.upsert_spcurve(cpcurve)
        dynamodb.upsert_spcurve(rfcurve)
        dynamodb.upsert_spcurve(spcurve1)
        dynamodb.upsert_spcurve(spcurve2)
        dynamodb.upsert_spcurve(spcurve3)
        dynamodb.upsert_spcurve(spcurve4)
    
    
    res_vesselmaster = dynamodb.get_vesselmaster(imo)
    vesselmaster = util_vesselmaster(res_vesselmaster)
    
    res_datachannel = dynamodb.get_datachannel(imo)
    datachannel = util_datachannel(res_datachannel)
    
    res_spcurve = dynamodb.get_spcurve(imo)
    cpcurve, rfcurve, spcurve1, spcurve2, spcurve3, spcurve4 = util_spcurve(res_spcurve)
    
    fuel_oil_type_list = dynamodb.get_fuel_oil_type()
    vessel_type_list = dynamodb.get_vessel_type()
    
    data = {
        "vesselmaster": vesselmaster,
        "fuel_oil_type_list": fuel_oil_type_list,
        "vessel_type_list": vessel_type_list,
        "datachannel": datachannel,
        "cpcurve": cpcurve,
        "rfcurve": rfcurve,
        "spcurve1": spcurve1,
        "spcurve2": spcurve2,
        "spcurve3": spcurve3,
        "spcurve4": spcurve4,
    }
    data = json.dumps(data)
    print(f"data: {data}")
        
    # TODO implement
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': data
    }
    