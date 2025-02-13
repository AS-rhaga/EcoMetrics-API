
import ast
from datetime import datetime
import json
import math
import re

import auth
from dynamodb import insert, select, delete

# datetime型のstartとendの時間差を返却。30分以上の場合は繰り上がり。
def calc_time_diff(start_time, end_time):

    return_val = 0
                        
    time_difference = end_time - start_time
    hours_difference = time_difference.total_seconds() / 3600

    # 30分以上で繰り上げ
    if hours_difference % 1 >= 0.5:
        return_val = math.ceil(hours_difference)  # 繰り上げ
    else:
        return_val = math.floor(hours_difference)  # 切り捨て
    
    return return_val

# FuelListをリストに変換
def convertFuelOileStringToList(text):
    
    pattern = r'\([^()]*\([^()]*\)[^()]*\)|\([^()]*\)'
    matches = re.findall(pattern, text)

    # 前後の括弧を除去
    cleaned_matches = [match[1:-1] for match in matches]

    return cleaned_matches

def lambda_handler(event, context):
    print(f"event{type(event)}: {event}")
    
    body = event['body']
    print(f"body{type(body)}: {body}")
    token = event['headers']['Authorization']
    
    edit_conditions_list = json.loads(body)
    imo = edit_conditions_list[0]["imo"]
    
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

    # 既存Simulationテーブル削除
    delete.delete_simulation(imo)

    # 既存Simulationテーブル登録
    for item in edit_conditions_list:
        insert.upsert_simulation(imo, item)

    # Simulationテーブル取得
    res_simulation = select.get_simulation(imo)

    # FOC Formulas取得
    res_foc_formulas = select.get_foc_formulas(imo)
    print(f"res_foc_formulas:{res_foc_formulas}")

    # 返却値設定
    data_list = []
    for res_item in res_simulation:

        print(f"res_item{type(res_item)}: {res_item}")

        # Leg No 取得
        tmp_text = res_item["year_and_serial_number"]["S"]
        start_index = tmp_text.find('E')
        leg_no = tmp_text[start_index:]  # 'E' 以降を抽出

        # total time算出
        # DepartureTime取得
        departure_time_string = res_item["departure_time"]["S"]
        departure_time = datetime.strptime(departure_time_string, "%Y/%m/%d %H:%M")

        # ArrivalTime取得
        arrival_time_string = res_item["arrival_time"]["S"]
        arrival_time = datetime.strptime(arrival_time_string, "%Y/%m/%d %H:%M")

        # Leg航海時間算出
        total_time = calc_time_diff(departure_time, arrival_time)
        print(f"total_time{type(total_time)}: {total_time}")

        # Log Speed算出
        log_speed = int(res_item["distance"]["S"]) / total_time

        # FOC算出（FOC Formulasが取得出来なかった場合は計算しない）
        if res_foc_formulas:

            # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
            auxiliary_equipment = float(res_foc_formulas[0]["auxiliary_equipment"]["S"])
            print(f"auxiliary_equipment: {(auxiliary_equipment)}")

            # FOC算出時にBallast/Ladenどちらの式を使うかを判定
            if res_item["dispracement"]["S"] == "Ballast":
                # Ballast用の計算パラメータを取得し、FOCを算出
                calc_param_list = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])

            else:
                # 上記以外の場合（実質Laden）                       
                # Laden用の計算パラメータを取得し、FOCを算出
                calc_param_list = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])

            alpah = calc_param_list[0]
            a = calc_param_list[1]
            c = calc_param_list[2]

            # 1日あたりのFOC算出（**は指数）
            foc_per_day = alpah * log_speed ** a + c + auxiliary_equipment
            # 1時間あたりのFOC算出
            foc_per_hour = foc_per_day / 24
            # 総FOCを算出
            foc = round(float(foc_per_hour * total_time), 1)
        else:
            foc = "-"
        
        #Fuel取得
        output_fuel_list = []
        fuel_list = convertFuelOileStringToList(res_item["fuel"]["S"]) 

        for fuel in fuel_list:
            fuel_info_list = fuel.split(',')

            output_fuel = {
                "fuel_type" : fuel_info_list[0],
                "fuel_rate" : fuel_info_list[1],
            }

            output_fuel_list.append(output_fuel)

        data = {
            "leg_no"            : leg_no,
            "departure_port"    : res_item["departure_port"]["S"],
            "departure_time"    : res_item["departure_time"]["S"],
            "arrival_port"      : res_item["arrival_port"]["S"],
            "arrival_time"      : res_item["arrival_time"]["S"],
            "total_time"        : str(total_time),
            "distance"          : res_item["distance"]["S"],
            "fuel"              : output_fuel_list,
            "dispracement"      : res_item["dispracement"]["S"],
            "log_speed"         : round(float(log_speed), 1),
            "foc"               : str(foc),
        }

        data_list.append(data)

    # ソート実行-------------------------------------------------------
    new_data_list = sorted(data_list, key=lambda x: x['leg_no'])

    datas = {
        "datas": new_data_list
    }

    datas = json.dumps(datas)
    print(f"datas{type(datas)}: {datas}")

    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        'body': datas
    }
    