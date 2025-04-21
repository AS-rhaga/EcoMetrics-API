import ast
from datetime import datetime
import math
import re

import boto3
import os

__dynamodb_client = boto3.client('dynamodb')
__table_name_year_total          = os.environ['YEAR_TOTAL']
__table_name_foc_formulas        = os.environ['FOC_FORMULAS']
__table_name_simulation_voyage   = os.environ['EU_SIMULTATION_VOYAGE_PLAN']
__table_name_simulation_speed    = os.environ['EU_SIMULTATION_SPEED_PLAN']
__table_name_fuel_oil_type       = os.environ['FUEL_OIL_TYPE']

def get_year_total_by_imo(imo):
    data = []
    response = __dynamodb_client.query(
        TableName=__table_name_year_total,
        ExpressionAttributeNames={
            '#name0': 'imo'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    data = response['Items']
    
    count = 0
    while 'LastEvaluatedKey' in response:
        response = __dynamodb_client.query(
            TableName=__table_name_year_total,
            ExpressionAttributeNames={
                '#name0': 'imo'
            },
            ExpressionAttributeValues={
                ':value0': {'S': imo}
            },
            KeyConditionExpression='#name0 = :value0',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
        count += 1

    return data

def get_foc_formulas(imo):

    response = __dynamodb_client.query(
        TableName=__table_name_foc_formulas,
        ExpressionAttributeNames={
            '#name0': 'imo'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo}
        },
        KeyConditionExpression='#name0 = :value0'
    )
    # データが存在するか確認
    if 'Items' in response:
        return response['Items']
    else:
        return None

def get_simulation_voyage(imo, year):

    response = __dynamodb_client.query(
        TableName=__table_name_simulation_voyage,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'year_and_serial_number'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': year}
        },
        KeyConditionExpression='#name0 = :value0 AND begins_with(#name1, :value1)'
    )

    # データが存在するか確認
    if 'Items' in response:
        return response['Items']
    else:
        return None       

def get_simulation_speed(imo, year):

    response = __dynamodb_client.query(
        TableName=__table_name_simulation_speed,
        ExpressionAttributeNames={
            '#name0': 'imo',
            '#name1': 'year'
        },
        ExpressionAttributeValues={
            ':value0': {'S': imo},
            ':value1': {'S': year}
        },
        KeyConditionExpression='#name0 = :value0 AND #name1 = :value1'
    )
    # データが存在するか確認
    if 'Items' in response:
        return response['Items']
    else:
        return None

def get_fuel_oil_type():
    data = []
    response = __dynamodb_client.scan(
        TableName=__table_name_fuel_oil_type
    )
    data = response['Items']
    return data

def make_fuel_oil_type_list():
    
    # fuel-oil-typeリストを取得
    fuel_oil_type_info_list = get_fuel_oil_type()

    for i in range(len(fuel_oil_type_info_list)):
        name = fuel_oil_type_info_list[i]["fuel_oil_type"]["S"]
        if  name == "LNG(Otto Diesel Speed)":
            LNG_ODS_info_list = fuel_oil_type_info_list[i]
        elif  name == "LNG(Otto Medium Speed)":
            LNG_OMS_info_list = fuel_oil_type_info_list[i]
        elif  name == "LNG(Otto Slow Speed)":
            LNG_OSS_info_list = fuel_oil_type_info_list[i]
        elif name == "HFO":
            HFO_info_list = fuel_oil_type_info_list[i]
        elif name == "LFO":
            LFO_info_list = fuel_oil_type_info_list[i]
        elif name == "MDO":
            MDO_info_list = fuel_oil_type_info_list[i]
        elif name == "MGO":
            MGO_info_list = fuel_oil_type_info_list[i]
        elif name == "LPG(Propane)":
            LPG_Propane_info_list = fuel_oil_type_info_list[i]
        elif name == "LPG(Butane)":
            LPG_Butane_info_list = fuel_oil_type_info_list[i]
        elif name == "NH3(Natural gas)":
            NH3_Natural_Gas_info_list = fuel_oil_type_info_list[i]
        elif name == "NH3(e-fuel)":
            NH3_eFuel_info_list = fuel_oil_type_info_list[i]
        elif name == "Methanol(Natural gas)":
            Methanol_Natural_Gas_info_list = fuel_oil_type_info_list[i]
        elif name == "H2(Natural gas)":
            H2_Natural_Gas_info_list = fuel_oil_type_info_list[i]

    fuel_oil_type_list = {
        "LNG_ODS_info_list"             : LNG_ODS_info_list, 
        "LNG_OMS_info_list"             : LNG_OMS_info_list, 
        "LNG_OSS_info_list"             : LNG_OSS_info_list, 
        "HFO_info_list"                 : HFO_info_list, 
        "LFO_info_list"                 : LFO_info_list, 
        "MDO_info_list"                 : MDO_info_list, 
        "MGO_info_list"                 : MGO_info_list, 
        "LPG_Propane_info_list"         : LPG_Propane_info_list, 
        "LPG_Butane_info_list"          : LPG_Butane_info_list, 
        "NH3_Ng_info_list"              : NH3_Natural_Gas_info_list, 
        "NH3_eFuel_info_list"           : NH3_eFuel_info_list, 
        "Methanol_Ng_info_list"         : Methanol_Natural_Gas_info_list, 
        "H2_Ng_info_list"               : H2_Natural_Gas_info_list
    }

    return fuel_oil_type_list

def to_datetime(str_time):

    dt_time = datetime(
        year   = int(str_time[0:4]),
        month  = int(str_time[5:7]),
        day    = int(str_time[8:10]),
        hour   = int(str_time[11:13]),
        minute = int(str_time[14:16])
    )
    return dt_time

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

# CO2排出量の算出メソッド
def calc_co2(year, lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng, fuel_oil_type_info_list):

    # EUAの算出
    co2_total   = 0
    eu_ets_rate = 0

    # EU-ETS対象割合を確認
    if year == "2024":
        eu_ets_rate = 40
    elif year == "2025":
        eu_ets_rate = 70
    else:
        eu_ets_rate = 100

    # print(f"eu_ets_rate: {(eu_ets_rate)}")
    if lng_ods > 0:
        lng_ods_co2_factor =  float(fuel_oil_type_info_list["LNG_ODS_info_list"]["emission_factor"]["S"])
        co2_total += lng_ods * lng_ods_co2_factor
    if lng_oms > 0:
        lng_oms_co2_factor =  float(fuel_oil_type_info_list["LNG_OMS_info_list"]["emission_factor"]["S"])
        co2_total += lng_oms * lng_oms_co2_factor
    if lng_oss > 0:
        lng_oss_co2_factor =  float(fuel_oil_type_info_list["LNG_OSS_info_list"]["emission_factor"]["S"])
        co2_total += lng_oss * lng_oss_co2_factor
    if hfo > 0:
        hfo_co2_factor =  float(fuel_oil_type_info_list["HFO_info_list"]["emission_factor"]["S"])
        co2_total += hfo * hfo_co2_factor
    if lfo > 0:
        lfo_co2_factor =  float(fuel_oil_type_info_list["LFO_info_list"]["emission_factor"]["S"])
        co2_total += lfo * lfo_co2_factor
    if mdo > 0:
        mdo_co2_factor =  float(fuel_oil_type_info_list["MDO_info_list"]["emission_factor"]["S"])
        co2_total += mdo * mdo_co2_factor
    if mgo > 0:
        mgo_co2_factor =  float(fuel_oil_type_info_list["MGO_info_list"]["emission_factor"]["S"])
        co2_total += mgo * mgo_co2_factor
    if lpg_p > 0:
        lpg_p_co2_factor = float(fuel_oil_type_info_list["LPG_Propane_info_list"]["emission_factor"]["S"])
        co2_total += lpg_p * lpg_p_co2_factor
    if lpg_b > 0:
        lpg_b_co2_factor = float(fuel_oil_type_info_list["LPG_Butane_info_list"]["emission_factor"]["S"])
        co2_total += lpg_b * lpg_b_co2_factor
    if nh3_ng > 0:
        nh3_ng_co2_factor = float(fuel_oil_type_info_list["NH3_Ng_info_list"]["emission_factor"]["S"])
        co2_total += nh3_ng * nh3_ng_co2_factor
    if nh3_ef > 0:
        nh3_ef_co2_factor = float(fuel_oil_type_info_list["NH3_eFuel_info_list"]["emission_factor"]["S"])
        co2_total += nh3_ef * nh3_ef_co2_factor
    if methanol_ng > 0:
        methanol_ng_co2_factor = float(fuel_oil_type_info_list["Methanol_Ng_info_list"]["emission_factor"]["S"])
        co2_total += methanol_ng * methanol_ng_co2_factor
    if h2_ng > 0:
        h2_ng_co2_factor = float(fuel_oil_type_info_list["H2_Ng_info_list"]["emission_factor"]["S"])
        co2_total += h2_ng * h2_ng_co2_factor
        
    return co2_total

# EUAの算出メソッド
def calc_eua(year, total_co2):

    # EUAの算出
    eu_ets_rate = 0
    eua = 0

    # EU-ETS対象割合を確認
    if year == "2024":
        eu_ets_rate = 40
    elif year == "2025":
        eu_ets_rate = 70
    else:
        eu_ets_rate = 100
    # print(f"eu_ets_rate: {(eu_ets_rate)}")

    eua       = total_co2 * float(eu_ets_rate) / 100
    # print(f"eua{type(eua)}: {eua}")
    return eua

# エネルギーの総消費量を算出するメソッド
def calc_energy(lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng, fuel_oil_type_list):
    total_energy = 0

    if lng_ods > 0:
        lng_ods_lcv =  float(fuel_oil_type_list["LNG_ODS_info_list"]["lcv"]["S"])
        total_energy += lng_ods * lng_ods_lcv
    if lng_oms > 0:
        lng_oms_lcv =  float(fuel_oil_type_list["LNG_OMS_info_list"]["lcv"]["S"])
        total_energy += lng_oms * lng_oms_lcv
    if lng_oss > 0:
        lng_oss_lcv =  float(fuel_oil_type_list["LNG_OSS_info_list"]["lcv"]["S"])
        total_energy += lng_oss * lng_oss_lcv
    if hfo > 0:
        hfo_lcv =  float(fuel_oil_type_list["HFO_info_list"]["lcv"]["S"])
        total_energy += hfo * hfo_lcv
    if lfo > 0:
        lfo_lcv =  float(fuel_oil_type_list["LFO_info_list"]["lcv"]["S"])
        total_energy += lfo * lfo_lcv
    if mdo > 0:
        mdo_lcv =  float(fuel_oil_type_list["MDO_info_list"]["lcv"]["S"])
        total_energy += mdo * mdo_lcv
    if mgo > 0:
        mgo_lcv =  float(fuel_oil_type_list["MGO_info_list"]["lcv"]["S"])
        total_energy += mgo * mgo_lcv
    if lpg_p > 0:
        lpg_p_lcv = float(fuel_oil_type_list["LPG_Propane_info_list"]["lcv"]["S"])
        total_energy += lpg_p * lpg_p_lcv
    if lpg_b > 0:
        lpg_b_lcv = float(fuel_oil_type_list["LPG_Butane_info_list"]["lcv"]["S"])
        total_energy += lpg_b * lpg_b_lcv
    if nh3_ng > 0:
        nh3_ng_lcv = float(fuel_oil_type_list["NH3_Ng_info_list"]["lcv"]["S"])
        total_energy += nh3_ng * nh3_ng_lcv
    if nh3_ef > 0:
        nh3_ef_lcv = float(fuel_oil_type_list["NH3_eFuel_info_list"]["lcv"]["S"])
        total_energy += nh3_ef * nh3_ef_lcv
    if methanol_ng > 0:
        methanol_ng_lcv = float(fuel_oil_type_list["Methanol_Ng_info_list"]["lcv"]["S"])
        total_energy += methanol_ng * methanol_ng_lcv
    if h2_ng > 0:
        h2_ng_lcv = float(fuel_oil_type_list["H2_Ng_info_list"]["lcv"]["S"])
        total_energy += h2_ng * h2_ng_lcv

    return_energy = total_energy

    return return_energy

#実際のGHG強度を算出するメソッド
def calc_GHG_Actual(lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng, fuel_oil_type_list):
    sum_ghg = 0
    sum_foc = 0

    if lng_ods > 0:
        lng_ghg_ods_intensity =  float(fuel_oil_type_list["LNG_ODS_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lng_ods * lng_ghg_ods_intensity
        sum_foc += lng_ods
    if lng_oms > 0:
        lng_ghg_oms_intensity =  float(fuel_oil_type_list["LNG_OMS_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lng_oms * lng_ghg_oms_intensity
        sum_foc += lng_oms
    if lng_oss > 0:
        lng_ghg_oss_intensity =  float(fuel_oil_type_list["LNG_OSS_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lng_oss * lng_ghg_oss_intensity
        sum_foc += lng_oss
    if hfo > 0:
        hfo_ghg_intensity =  float(fuel_oil_type_list["HFO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += hfo * hfo_ghg_intensity
        sum_foc += hfo
    if lfo > 0:
        lfo_ghg_intensity =  float(fuel_oil_type_list["LFO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lfo * lfo_ghg_intensity
        sum_foc += lfo
    if mdo > 0:
        mdo_ghg_intensity =  float(fuel_oil_type_list["MDO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += mdo * mdo_ghg_intensity
        sum_foc += mdo
    if mgo > 0:
        mgo_ghg_intensity =  float(fuel_oil_type_list["MGO_info_list"]["ghg_intensity"]["S"])
        sum_ghg += mgo * mgo_ghg_intensity
        sum_foc += mgo
    if lpg_p > 0:
        lpg_p_ghg_intensity =  float(fuel_oil_type_list["LPG_Propane_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lpg_p * lpg_p_ghg_intensity
        sum_foc += lpg_p
    if lpg_b > 0:
        lpg_b_ghg_intensity =  float(fuel_oil_type_list["LPG_Butane_info_list"]["ghg_intensity"]["S"])
        sum_ghg += lpg_b * lpg_b_ghg_intensity
        sum_foc += lpg_b
    if nh3_ng > 0:
        nh3_ng_ghg_intensity =  float(fuel_oil_type_list["NH3_Ng_info_list"]["ghg_intensity"]["S"])
        sum_ghg += nh3_ng * nh3_ng_ghg_intensity
        sum_foc += nh3_ng
    if nh3_ef > 0:
        nh3_ef_ghg_intensity =  float(fuel_oil_type_list["NH3_eFuel_info_list"]["ghg_intensity"]["S"])
        sum_ghg += nh3_ef * nh3_ef_ghg_intensity
        sum_foc += nh3_ef
    if methanol_ng > 0:
        methanol_ng_ghg_intensity =  float(fuel_oil_type_list["Methanol_Ng_info_list"]["ghg_intensity"]["S"])
        sum_ghg += methanol_ng * methanol_ng_ghg_intensity
        sum_foc += methanol_ng
    if h2_ng > 0:
        h2_ng_ghg_intensity =  float(fuel_oil_type_list["H2_Ng_info_list"]["ghg_intensity"]["S"])
        sum_ghg += h2_ng * h2_ng_ghg_intensity
        sum_foc += h2_ng

    GHG_Actual = 0
    if sum_foc > 0:
        GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    # print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual

def calc_GHG_Max(year):
    year = int(year)
    if year <= 2024:
        target_rate = 0
    elif year <= 2029:
        target_rate = 2
    elif year <= 2034:
        target_rate = 6
    elif year <= 2039:
        target_rate = 14.5
    elif year <= 2044:
        target_rate = 31
    elif year <= 2049:
        target_rate = 62
    else:
        target_rate = 80

    GHG_Max = round(float(91.16 * (100 - float(target_rate)) / 100), 2)
    # print(f"GHG_Max{type(GHG_Max)}: {GHG_Max}")
    return GHG_Max

# コンプライアンスバランスを算出するメソッド
def calc_cb(year_timestamp, energy, GHG_Actual):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    cb = (GHG_Max - GHG_Actual) * energy
    # print(f"cb{type(cb)}: {cb}")

    return cb

def make_voyage_plans_data(thisyear_year_total, voyage_plan_list, res_foc_formulas, fuel_oil_type_list, ytd_energy):

    # 各種合計値用変数のセット
    total_lng_oms     = 0
    total_lng_oss     = 0
    total_lng_ods     = 0
    total_hfo         = 0
    total_lfo         = 0
    total_mdo         = 0
    total_mgo         = 0
    total_lpg_p       = 0
    total_lpg_b       = 0
    total_h2_ng       = 0
    total_nh3_ng      = 0
    total_methanol_ng = 0
    total_nh3_ef      = 0
    total_foc         = 0
    total_distance    = 0
    total_eua         = 0
    total_energy      = 0
    total_cb          = 0

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)
    str_now = dt_now.strftime('%Y-%m-%dT%H:%M')

    if thisyear_year_total:
        # 実績値を合計値用変数に加算
        total_lng_oms  += float(thisyear_year_total["total_lng"]["S"])
        total_hfo      += float(thisyear_year_total["total_hfo"]["S"])
        total_lfo      += float(thisyear_year_total["total_lfo"]["S"])
        total_mdo      += float(thisyear_year_total["total_mdo"]["S"])
        total_mgo      += float(thisyear_year_total["total_mgo"]["S"])
        total_foc      += float(thisyear_year_total["total_foc"]["S"])
        total_distance += float(thisyear_year_total["distance"]["S"])
        total_eua      += float(thisyear_year_total["eua"]["S"])
        total_energy   += ytd_energy

    for i in range(len(voyage_plan_list)):

        # 変数の設定
        leg_total_time          = 0
        simulation_leg_lng      = 0
        simulation_leg_hfo      = 0
        simulation_leg_lfo      = 0
        simulation_leg_mdo      = 0
        simulation_leg_mgo      = 0
        simulation_leg_lpg_p    = 0
        simulation_leg_ethanol  = 0
        simulation_leg_lpg_b    = 0
        simulation_leg_methanol = 0
        simulation_foc_per_day  = 0

        # legの開始・終了時刻を取得する
        str_departure_time = voyage_plan_list[i]["departure_time"]["S"]     # "2024-12-10 12:30"
        str_arrival_time   = voyage_plan_list[i]["arrival_time"]["S"]       # "2024-12-19 17:30"

        # legの開始・終了時刻からlegの時間を算出する
        dt_departure_time = to_datetime(str_departure_time)
        dt_arrival_time = to_datetime(str_arrival_time)
        leg_total_time = calc_time_diff(dt_departure_time, dt_arrival_time)

        # 各項目を取得
        displacement           = voyage_plan_list[i]["dispracement"]["S"]
        leg_distance        = float(voyage_plan_list[i]["distance"]["S"])
        leg_eu_rate                = int(voyage_plan_list[i]["eu_rate"]["S"])

        # log_speedを算出
        leg_log_speed = 0
        if leg_total_time != 0:
            leg_log_speed = leg_distance / leg_total_time

        if res_foc_formulas:

            # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
            auxiliary_equipment = float(res_foc_formulas[0]["auxiliary_equipment"]["S"])

            # Ballast、Ladenどちらか判断して、FOCを算出
            if displacement == "Ballast":
                # Ballast用の計算パラメータを取得し、1日当たりのFOCを算出
                calc_balast_param_list = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])
                ballast_alpha = calc_balast_param_list[0]
                ballast_a = calc_balast_param_list[1]
                ballast_c = calc_balast_param_list[2]
                simulation_foc_per_day = ballast_alpha * leg_log_speed ** ballast_a + ballast_c + auxiliary_equipment
            else:
                # Laden用の計算パラメータを取得し、1日当たりのFOCを算出
                calc_laden_param_list = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])
                laden_alpha = calc_laden_param_list[0]
                laden_a = calc_laden_param_list[1]
                laden_c = calc_laden_param_list[2]
                simulation_foc_per_day = laden_alpha * leg_log_speed ** laden_a + laden_c + auxiliary_equipment

            # 1時間あたりのFOC算出
            simulation_foc_per_hour = simulation_foc_per_day / 24
            # Leg内総FOCを算出
            simulation_leg_foc = simulation_foc_per_hour * leg_total_time * leg_eu_rate / 100

            # 燃料別消費量を算出する
            fuel_list = convertFuelOileStringToList(voyage_plan_list[i]["fuel"]["S"])  

            # EU Rate考慮済の燃料消費量
            simulation_leg_lng_oms = 0
            simulation_leg_lng_oss = 0
            simulation_leg_lng_ods = 0
            simulation_leg_hfo = 0
            simulation_leg_lfo = 0
            simulation_leg_mdo = 0
            simulation_leg_mgo = 0
            simulation_leg_lpg_p = 0
            simulation_leg_lpg_b = 0
            simulation_leg_h2_ng = 0
            simulation_leg_nh3_ng = 0
            simulation_leg_methanol_ng = 0
            simulation_leg_nh3_ef = 0

            for fuel in fuel_list:
                fuel_info_list = fuel.split(',')
                fuel_type = fuel_info_list[0]
                fuel_rate = int(fuel_info_list[1])

                if  fuel_type == "LNG(Otto Diesel Speed)":
                    simulation_leg_lng_ods = simulation_leg_foc * int(fuel_rate) / 100
                    total_lng_ods     += simulation_leg_lng_ods
                elif  fuel_type == "LNG(Otto Medium Speed)":
                    simulation_leg_lng_oms = simulation_leg_foc * int(fuel_rate) / 100
                    total_lng_oms     += simulation_leg_lng_oms
                elif  fuel_type == "LNG(Otto Slow Speed)":
                    simulation_leg_lng_oss = simulation_leg_foc * int(fuel_rate) / 100
                    total_lng_oss     += simulation_leg_lng_oss
                elif fuel_type == "HFO":
                    simulation_leg_hfo = simulation_leg_foc * int(fuel_rate) / 100
                    total_hfo          += simulation_leg_hfo
                elif fuel_type == "LFO":
                    simulation_leg_lfo = simulation_leg_foc * int(fuel_rate) / 100
                    total_lfo         += simulation_leg_lfo
                elif fuel_type == "MDO":
                    simulation_leg_mdo = simulation_leg_foc * int(fuel_rate) / 100
                    total_mdo         += simulation_leg_mdo
                elif fuel_type == "MGO":
                    simulation_leg_mgo = simulation_leg_foc * int(fuel_rate) / 100
                    total_mgo         += simulation_leg_mgo
                elif fuel_type == "LPG(Propane)":
                    simulation_leg_lpg_p = simulation_leg_foc * int(fuel_rate) / 100
                    total_lpg_p         += simulation_leg_lpg_p
                elif fuel_type == "LPG(Butane)":
                    simulation_leg_lpg_b = simulation_leg_foc * int(fuel_rate) / 100
                    total_lpg_b         += simulation_leg_lpg_b
                elif fuel_type == "NH3(Natural gas)":
                    simulation_leg_nh3_ng = simulation_leg_foc * int(fuel_rate) / 100
                    total_nh3_ng         += simulation_leg_nh3_ng
                elif fuel_type == "NH3(e-fuel)":
                    simulation_leg_nh3_ef = simulation_leg_foc * int(fuel_rate) / 100
                    total_nh3_ef         += simulation_leg_nh3_ef
                elif fuel_type == "Methanol(Natural gas)":
                    simulation_leg_methanol_ng = simulation_leg_foc * int(fuel_rate) / 100
                    total_methanol_ng         += simulation_leg_methanol_ng
                elif fuel_type == "H2(Natural gas)":
                    simulation_leg_h2_ng = simulation_leg_foc * int(fuel_rate) / 100
                    total_h2_ng         += simulation_leg_h2_ng

            # シミュレーション部分で実際に排出したco2を算出する
            simulation_leg_co2 = calc_co2(now_year, simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)
            # シミュレーション部分のEUAを算出する
            simulation_leg_eua = calc_eua(now_year, simulation_leg_co2)

            # シミュレーション部分のエネルギー総消費量を算出する
            simulation_energy  = calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)
            total_energy      += simulation_energy

            # 合計用変数に加算する
            total_distance += leg_distance
            total_foc      += (simulation_leg_lng + simulation_leg_hfo + simulation_leg_lfo + simulation_leg_mdo + simulation_leg_mgo + simulation_leg_lpg_p + simulation_leg_ethanol + simulation_leg_lpg_b + simulation_leg_methanol)
            total_eua      += simulation_leg_eua
        
    # 最終的なCBを算出
    total_GHG = calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_list)
    total_cb  = calc_cb(now_year, total_energy, total_GHG)

    # Voyage Planのシミュレーション用データ
    dataset = {
        "eoy_eua"        : total_eua,
        "eoy_cb"         : total_cb,
    }
    
    return dataset

def make_speed_plans_data(thisyear_year_total, speed_plan, res_foc_formulas, fuel_oil_type_list, ytd_energy):

    # 各種合計値用変数のセット
    total_lng_oms     = 0
    total_lng_oss     = 0
    total_lng_ods     = 0
    total_hfo         = 0
    total_lfo         = 0
    total_mdo         = 0
    total_mgo         = 0
    total_lpg_p       = 0
    total_lpg_b       = 0
    total_h2_ng       = 0
    total_nh3_ng      = 0
    total_methanol_ng = 0
    total_nh3_ef      = 0
    total_foc         = 0
    total_distance    = 0
    total_eua         = 0
    total_energy      = 0
    total_cb          = 0

    # 処理実施時の年、日付を取得
    dt_now = datetime.now()
    now_year = str(dt_now.year)

    if thisyear_year_total:
        # 実績値を合計値用変数に加算
        total_lng_oms  += float(thisyear_year_total["total_lng"]["S"])
        total_hfo      += float(thisyear_year_total["total_hfo"]["S"])
        total_lfo      += float(thisyear_year_total["total_lfo"]["S"])
        total_mdo      += float(thisyear_year_total["total_mdo"]["S"])
        total_mgo      += float(thisyear_year_total["total_mgo"]["S"])
        total_foc      += float(thisyear_year_total["total_foc"]["S"])
        total_distance += float(thisyear_year_total["distance"]["S"])
        total_eua      += float(thisyear_year_total["eua"]["S"])
        total_energy   += ytd_energy

    simulation_leg_lng      = 0
    simulation_leg_hfo      = 0
    simulation_leg_lfo      = 0
    simulation_leg_mdo      = 0
    simulation_leg_mgo      = 0
    simulation_leg_lpg_p    = 0
    simulation_leg_ethanol  = 0
    simulation_leg_lpg_b    = 0
    simulation_leg_methanol = 0

    # SpeedPlanのSimulation処理実施   
    # Time to End of Year算出（年末 - 現在）
    year_end = datetime(dt_now.year, 12, 31, 23, 59, 59)
    time_to_end_of_year = calc_time_diff(dt_now, year_end)

    # 航海時間を算出
    sailing_rate = float(speed_plan[0]["salling_rate"]["S"])
    sailing_time = time_to_end_of_year * (sailing_rate / 100)
    port_time    = time_to_end_of_year - sailing_time
    print(f"sailing_time:{(sailing_time)} port_time:{(port_time)}")

    # Ballast、Ladenそれぞれの航海距離を算出
    displacement_rate    = float(speed_plan[0]["dispracement_rate"]["S"])
    ballast_sailing_time = sailing_time * (displacement_rate / 100)
    laden_sailing_time   = sailing_time - ballast_sailing_time

    # 時間×速さで距離を算出
    ballast_logspeed = float(speed_plan[0]["log_speed_ballast"]["S"])
    laden_logspeed   = float(speed_plan[0]["log_speed_laden"]["S"])
    ballast_ditance  = ballast_sailing_time * ballast_logspeed
    laden_distance    = laden_sailing_time * laden_logspeed

    # BallastDisancen、LadenDistanceを加算
    total_ballast_laden_distance = ballast_ditance + laden_distance

    leg_eu_rate = int(speed_plan[0]["eu_rate"]["S"])

    if res_foc_formulas: 

        # auxiliary_equipment（いつでも加算する燃料消費量）を考慮
        auxiliary_equipment = float(res_foc_formulas[0]["auxiliary_equipment"]["S"])

        # Ballast用の計算パラメータを取得し、1日当たりのFOCを算出
        calc_balast_param_list = ast.literal_eval(res_foc_formulas[0]["me_ballast"]["S"])
        ballast_alpha = calc_balast_param_list[0]
        ballast_a = calc_balast_param_list[1]
        ballast_c = calc_balast_param_list[2]
        ballast_foc_per_day = ballast_alpha * ballast_logspeed ** ballast_a + ballast_c + auxiliary_equipment
        # Laden用の計算パラメータを取得し、1日当たりのFOCを算出
        calc_laden_param_list = ast.literal_eval(res_foc_formulas[0]["me_laden"]["S"])
        laden_alpha = calc_laden_param_list[0]
        laden_a = calc_laden_param_list[1]
        laden_c = calc_laden_param_list[2]
        laden_foc_per_day = laden_alpha * laden_logspeed ** laden_a + laden_c + auxiliary_equipment

        # 1時間あたりのFOC算出
        ballast_foc_per_hour = ballast_foc_per_day / 24
        laden_foc_per_hour = laden_foc_per_day / 24
        # FOC算出
        ballast_foc = ballast_foc_per_hour * ballast_sailing_time
        laden_foc = laden_foc_per_hour * laden_sailing_time
        # Leg内総FOCを算出
        simulation_salling_foc = (ballast_foc + laden_foc) * leg_eu_rate / 100
        simulation_port_foc    = auxiliary_equipment / 24 * port_time * leg_eu_rate / 100
        simulation_leg_foc     = simulation_salling_foc + simulation_port_foc
        print(f"simulation_leg_foc:{(simulation_leg_foc)}")
        
        # 燃料別消費量を算出する
        fuel_list = convertFuelOileStringToList(speed_plan[0]["fuel"]["S"]) 

        # EU Rate考慮済の燃料消費量
        simulation_leg_lng_oms = 0
        simulation_leg_lng_oss = 0
        simulation_leg_lng_ods = 0
        simulation_leg_hfo = 0
        simulation_leg_lfo = 0
        simulation_leg_mdo = 0
        simulation_leg_mgo = 0
        simulation_leg_lpg_p = 0
        simulation_leg_lpg_b = 0
        simulation_leg_h2_ng = 0
        simulation_leg_nh3_ng = 0
        simulation_leg_methanol_ng = 0
        simulation_leg_nh3_ef = 0

        for fuel in fuel_list:
            fuel_info_list = fuel.split(',')
            fuel_type = fuel_info_list[0]
            fuel_rate = fuel_info_list[1]

            if  fuel_type == "LNG(Otto Diesel Speed)":
                simulation_leg_lng_ods = simulation_leg_foc * int(fuel_rate) / 100
                total_lng_ods     += simulation_leg_lng_ods
            elif  fuel_type == "LNG(Otto Medium Speed)":
                simulation_leg_lng_oms = simulation_leg_foc * int(fuel_rate) / 100
                total_lng_oms     += simulation_leg_lng_oms
            elif  fuel_type == "LNG(Otto Slow Speed)":
                simulation_leg_lng_oss = simulation_leg_foc * int(fuel_rate) / 100
                total_lng_oss     += simulation_leg_lng_oss
            elif fuel_type == "HFO":
                simulation_leg_hfo = simulation_leg_foc * int(fuel_rate) / 100
                total_hfo          += simulation_leg_hfo
            elif fuel_type == "LFO":
                simulation_leg_lfo = simulation_leg_foc * int(fuel_rate) / 100
                total_lfo         += simulation_leg_lfo
            elif fuel_type == "MDO":
                simulation_leg_mdo = simulation_leg_foc * int(fuel_rate) / 100
                total_mdo         += simulation_leg_mdo
            elif fuel_type == "MGO":
                simulation_leg_mgo = simulation_leg_foc * int(fuel_rate) / 100
                total_mgo         += simulation_leg_mgo
            elif fuel_type == "LPG(Propane)":
                simulation_leg_lpg_p = simulation_leg_foc * int(fuel_rate) / 100
                total_lpg_p         += simulation_leg_lpg_p
            elif fuel_type == "LPG(Butane)":
                simulation_leg_lpg_b = simulation_leg_foc * int(fuel_rate) / 100
                total_lpg_b         += simulation_leg_lpg_b
            elif fuel_type == "NH3(Natural gas)":
                simulation_leg_nh3_ng = simulation_leg_foc * int(fuel_rate) / 100
                total_nh3_ng         += simulation_leg_nh3_ng
            elif fuel_type == "NH3(e-fuel)":
                simulation_leg_nh3_ef = simulation_leg_foc * int(fuel_rate) / 100
                total_nh3_ef         += simulation_leg_nh3_ef
            elif fuel_type == "Methanol(Natural gas)":
                simulation_leg_methanol_ng = simulation_leg_foc * int(fuel_rate) / 100
                total_methanol_ng         += simulation_leg_methanol_ng
            elif fuel_type == "H2(Natural gas)":
                simulation_leg_h2_ng = simulation_leg_foc * int(fuel_rate) / 100
                total_h2_ng         += simulation_leg_h2_ng

        simulation_leg_co2 = calc_co2(now_year, simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)

        # シミュレーション部分のEUAを算出する
        simulation_leg_eua = calc_eua(now_year, simulation_leg_co2)

        # シミュレーション部分のエネルギー総消費量を算出する
        simulation_energy  = calc_energy(simulation_leg_lng_ods, simulation_leg_lng_oms, simulation_leg_lng_oss, simulation_leg_hfo, simulation_leg_lfo, simulation_leg_mdo, simulation_leg_mgo, simulation_leg_lpg_p, simulation_leg_lpg_b, simulation_leg_nh3_ng, simulation_leg_nh3_ef, simulation_leg_methanol_ng, simulation_leg_h2_ng, fuel_oil_type_list)
        total_energy      += simulation_energy

        # 合計用変数に加算する
        total_distance += total_ballast_laden_distance
        total_foc      += (simulation_leg_lng + simulation_leg_hfo + simulation_leg_lfo + simulation_leg_mdo + simulation_leg_mgo + simulation_leg_lpg_p + simulation_leg_ethanol + simulation_leg_lpg_b + simulation_leg_methanol)
        total_eua      += simulation_leg_eua

        # CB算出
        total_GHG = calc_GHG_Actual(total_lng_ods, total_lng_oms, total_lng_oss, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_lpg_b, total_nh3_ng, total_nh3_ef, total_methanol_ng, total_h2_ng, fuel_oil_type_list)
        total_cb  = calc_cb(now_year, total_energy, total_GHG)
        # print(f"simulation_energy:{simulation_energy}, ytd_energy:{ytd_energy}, total_energy:{total_energy}, total_GHG:{total_GHG}")

        # Voyage Planのシミュレーション用データ
        dataset = {
            "eoy_eua"        : total_eua,
            "eoy_cb"         : total_cb,
        }

    return dataset

def calc_eua_cb(imo):

    # 必要な変数・リストを作成
    last_year = 0
    voyage_flag = "0"
    speed_flag  = "0"

    # 現在の西暦4桁を取得する
    dt_now_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    year_now = dt_now_str[0:4]
    
    fuel_oil_type_list = make_fuel_oil_type_list()

    thisyear_year_total_list = []
    operator_total_list      = []

    # 返却用データセット
    return_data_list = []

    # imoのみをキーに、year-totalリストを取得
    total_year_total_list = get_year_total_by_imo(imo)

    # FOCFormulas取得
    res_foc_formulas = get_foc_formulas(imo)

    # シミュレーション用テーブルを取得
    simulation_plan_voyage_list = get_simulation_voyage(imo, year_now)
    simulation_plan_speed       = get_simulation_speed(imo, year_now)

    # どちらのSimulationを使用しているか確認
    if simulation_plan_voyage_list:
        voyage_flag = simulation_plan_voyage_list[0]["flag"]["S"] if "flag" in simulation_plan_voyage_list[0] else "0"
    
    # speed-planのシミュレーション使用フラグ確認
    if simulation_plan_speed:
        speed_flag = simulation_plan_speed[0]["flag"]["S"] if "flag" in simulation_plan_speed[0] else "0"

    # シミュレーションプラン管理用リスト
    ytd_exist_voyage_list     = []
    ytd_not_exist_voyage_list = []
    ytd_exist_speed_list      = []
    ytd_not_exist_speed_list  = []

    # 同一imoのyear-totalリストでループ
    for year_rec in total_year_total_list:

        # 処理対象年のレコードを抽出。
        if year_rec["year_and_ope"]["S"][0:4] == year_now:
            thisyear_year_total_list.append(year_rec)

    # 各種燃料の消費量と、消費エネルギーの合計値用変数を設定する。
    ytd_lng    = 0
    ytd_hfo    = 0
    ytd_lfo    = 0
    ytd_mdo    = 0
    ytd_mgo    = 0

    # 今年分のyear-totalレコード分ループ
    for rec in thisyear_year_total_list:

        # オペレータ
        operator = rec["year_and_ope"]["S"][4:50]
        
        # 昨年分のレコードを入れるリスト
        last_year_rec = []

        # print(f"total_year_total_list:{total_year_total_list}")
        # 同一imoのyear-totalリストでループ
        for year_rec in total_year_total_list:

            tmp_operator = year_rec["year_and_ope"]["S"][4:50]

            # 同一オペレータのレコードを抽出
            if tmp_operator == operator:
                operator_total_list.append(year_rec)

                # 西暦部分の確認、昨年のレコードであれば保持しておく。
                tmp_year = year_rec["year_and_ope"]["S"][0:4]
                if tmp_year == str(int(year_now) - 1):
                    last_year_rec = year_rec

        operator_total_list = sorted(operator_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

        # オペレーター別リストの中に昨年のレコードがあるかを確認する
        last_year = 0
        if len(last_year_rec) != 0:
            last_year_banking   = float(last_year_rec["banking"]["S"] if "banking" in last_year_rec and last_year_rec["banking"]["S"] != "" else "0")
            last_year_borrowing = float(last_year_rec["borrowing"]["S"] if "borrowing" in last_year_rec and last_year_rec["borrowing"]["S"] != "" else "0")

            if last_year_borrowing > 0:
                last_year = last_year_borrowing * (-1.1)
            elif last_year_banking > 0:
                last_year = last_year_banking
            else:
                last_year = 0

        # オペレーター別リストの今年のレコードから各項目を取得

        # 各項目を取得する
        lng       = float(rec["total_lng"]["S"])
        hfo       = float(rec["total_hfo"]["S"])
        lfo       = float(rec["total_lfo"]["S"])
        mdo       = float(rec["total_mdo"]["S"])
        mgo       = float(rec["total_mgo"]["S"])
        distance  = float(rec["distance"]["S"])
        eua       = float(rec["eua"]["S"])
        cb        = float(rec["cb"]["S"])
        banking   = float(rec["banking"]["S"] if "banking" in rec and rec["banking"]["S"] != "" else "0")
        borrowing = float(rec["borrowing"]["S"] if "borrowing" in rec and rec["borrowing"]["S"] != "" else "0")

        # CBから消費量エネルギー（EU Rate考慮済）を算出する
        energy     = calc_energy(0, lng, 0, hfo, lfo, mdo, mgo, 0, 0, 0, 0, 0, 0, fuel_oil_type_list)

        # 必要な計算を行う
        foc             = lng + hfo + lfo + mdo + mgo
        total_cb        = cb + borrowing + banking + last_year

        ytd_dataset = {
            "imo"            : imo,
            "operator"       : operator,
            "distance"       : distance,
            "foc"            : foc,
            "eua"            : eua,
            "cb"             : total_cb,
        }

        # 合計用変数に加算する。
        ytd_lng    += lng
        ytd_hfo    += hfo
        ytd_lfo    += lfo
        ytd_mdo    += mdo
        ytd_mgo    += mgo

        # シミュレーション結果を反映したEnd of Yearのデータを作成する。

        # voyage-planのシミュレーション使用フラグ確認
        if voyage_flag == "1":

            # ループ用に保持しておく
            keep_simulation_plan_voyage_list = simulation_plan_voyage_list.copy()

            # voyageのオペレーターを確認する
            for simulation_voyage in keep_simulation_plan_voyage_list:
                simulation_operator = simulation_voyage["operator"]["S"]

                # 基準のyear-totalレコードのオペレーターと一致する場合
                if simulation_operator == operator:
                    # 実測データが存在するリストに追加
                    ytd_exist_voyage_list.append(simulation_voyage)
                    # 各オペレーターで確認していった最後に、実測データ無しリスト入りにならないように
                    simulation_plan_voyage_list.remove(simulation_voyage)

            print(f"imo:{(imo)} ループ後len(ytd_exist_voyage_list):{(len(ytd_exist_voyage_list))}")
            print(f"imo:{(imo)} ループ後len(simulation_plan_voyage_list):{(len(simulation_plan_voyage_list))}")
        
        # speed-planのシミュレーション使用フラグ確認
        if speed_flag == "1":

            # オペレーターを確認する
            simulation_operator = simulation_plan_speed[0]["operator"]["S"]

            # 基準のyear-totalレコードのオペレーターと一致する場合
            if simulation_operator == operator:
                # 実測データが存在するリストに追加
                ytd_exist_speed_list.append(simulation_plan_speed[0])
                # 各オペレーターで確認していった最後に、実測データ無しリスト入りにならないように
                simulation_plan_speed.remove(simulation_plan_speed[0])
       
        # シミュレーションリストの処理
        # 実測データ有り かつ voyage-planの場合
        if len(ytd_exist_voyage_list) > 0:
            # VoyagePlanのシミュレーション実施
            eoy_vessel_data = make_voyage_plans_data(rec, ytd_exist_voyage_list, res_foc_formulas, fuel_oil_type_list, energy)

            # operatorが一致するytdデータと合わせて、データセットを作成
            dataset = {
                "ytd_eua"            : float(ytd_dataset["eua"]),
                "ytd_cb"             : float(ytd_dataset["cb"]),
                "eoy_eua"            : float(eoy_vessel_data["eoy_eua"]),
                "eoy_cb"             : float(eoy_vessel_data["eoy_cb"])
            }
            return_data_list.append(dataset)

        # 実測データ有りspeed-plan
        elif len(ytd_exist_speed_list) > 0:
            eoy_vessel_data = make_speed_plans_data(rec, ytd_exist_speed_list, res_foc_formulas, fuel_oil_type_list, energy)

            dataset = {              
                "ytd_eua"            : float(ytd_dataset["eua"]),
                "ytd_cb"             : float(ytd_dataset["cb"]),
                "eoy_eua"            : float(eoy_vessel_data["eoy_eua"]),
                "eoy_cb"             : float(eoy_vessel_data["eoy_cb"])
            }
            return_data_list.append(dataset)

        # 実測データあり かつ Simulationなし
        else:
            # Simulation無の場合は、ytdと同じ値をeoyに設定
            dataset = {
                "ytd_eua"            : float(ytd_dataset["eua"]),
                "ytd_cb"             : float(ytd_dataset["cb"]),
                "eoy_eua"            : float(ytd_dataset["eua"]),
                "eoy_cb"             : float(ytd_dataset["cb"]),
            }
            return_data_list.append(dataset)
    
    # year-totalループ終了後、各シミュレーションリストに残っているものは、実測データ無しオペレーター
    if voyage_flag == "1" and len(simulation_plan_voyage_list) > 0:
        for simulation_voyage in simulation_plan_voyage_list:
            # 実測データが存在しないリストに追加
            ytd_not_exist_voyage_list.append(simulation_voyage)
    if speed_flag == "1" and len(simulation_plan_speed) > 0:
        ytd_not_exist_speed_list.append(simulation_plan_speed[0])
    
    # 実測データ無しvoyage-plan
    if len(ytd_not_exist_voyage_list) > 0:

        # VoyagePlanのシミュレーション実施
        eoy_vessel_data = make_voyage_plans_data(None, ytd_not_exist_voyage_list, res_foc_formulas, fuel_oil_type_list, 0)

        print(f"imo:{imo}, eoy_eua:{eoy_vessel_data["eoy_eua"]}, eoy_cb:{eoy_vessel_data["eoy_cb"]}")

        # ytdは0、eoyはSimulation結果を設定
        dataset = {
            "ytd_eua"            : 0,
            "ytd_cb"             : 0,
            "eoy_eua"            : float(eoy_vessel_data["eoy_eua"]),
            "eoy_cb"             : float(eoy_vessel_data["eoy_cb"])
        }
        return_data_list.append(dataset)

        print(f"imo:{imo}, return_data_list:{return_data_list}")

    # 実測データ無しspeed-plan
    if len(ytd_not_exist_speed_list) > 0:

        eoy_vessel_data = make_speed_plans_data(None, ytd_not_exist_speed_list, res_foc_formulas, fuel_oil_type_list, 0)

        dataset = {              
            "ytd_eua"            : 0,
            "ytd_cb"             : 0,
            "eoy_eua"            : float(eoy_vessel_data["eoy_eua"]),
            "eoy_cb"             : float(eoy_vessel_data["eoy_cb"])
        }
        return_data_list.append(dataset)

    # 実測値、シミュレーションなし
    if len(return_data_list) == 0:
        dataset = {
            "ytd_eua"            : "0",
            "ytd_cb"             : "0",
            "eoy_eua"            : "0",
            "eoy_cb"             : "0",
        }
        return_data = dataset

    # 実測値、シミュレーションあり
    else:

        return_ytd_eua = 0
        return_eoy_eua = 0
        return_ytd_cb = 0
        return_eua_cb = 0

        # yearレコードごとの計算値を合算
        for data in return_data_list:

            return_ytd_eua += data["ytd_eua"]
            return_eoy_eua += data["eoy_eua"]
            return_ytd_cb += data["ytd_cb"]
            return_eua_cb += data["eoy_cb"]
        
        # 返却値用にセット
        dataset = {
            "ytd_eua"            : round(return_ytd_eua),
            "eoy_eua"            : round(return_eoy_eua),
            "ytd_cb"             : round(return_ytd_cb / 1000000, 1),
            "eoy_cb"             : round(return_eua_cb / 1000000, 1),
        }

        return_data = dataset
    
    print(f"imo:{imo}, return_data:{return_data}")

    return return_data
