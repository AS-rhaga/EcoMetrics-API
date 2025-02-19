
import ast
import math
from datetime import datetime

from dynamodb import select

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

def calc_borrowing_limit(thisYear_borrowing, year, energy):

    # 削減目標量
    target_rate = 0

    # 今年ボローイングOKかを確認
    if thisYear_borrowing == True:
    
        # GHG強度上限を算出
        GHG_Max = calc_GHG_Max(year)

        # borrowing_limitを算出
        borrowing_limit = GHG_Max * 0.02 * energy

    else:
        borrowing_limit = 0

    return borrowing_limit

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
    print(f"GHG_Max{type(GHG_Max)}: {GHG_Max}")
    return GHG_Max

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
        lng_ghg_oss_intensity =  float(fuel_oil_type_list["LNG_OMS_info_list"]["ghg_intensity"]["S"])
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
        nh3_ng_ghg_intensity =  float(fuel_oil_type_list["NH3_Natural_Gas_info_list"]["ghg_intensity"]["S"])
        sum_ghg += nh3_ng * nh3_ng_ghg_intensity
        sum_foc += nh3_ng
    if nh3_ef > 0:
        nh3_ef_ghg_intensity =  float(fuel_oil_type_list["NH3_eFuel_info_list"]["ghg_intensity"]["S"])
        sum_ghg += nh3_ef * nh3_ef_ghg_intensity
        sum_foc += nh3_ef
    if methanol_ng > 0:
        methanol_ng_ghg_intensity =  float(fuel_oil_type_list["Methanol_Natural_Gas_info_list"]["ghg_intensity"]["S"])
        sum_ghg += methanol_ng * methanol_ng_ghg_intensity
        sum_foc += methanol_ng
    if h2_ng > 0:
        h2_ng_ghg_intensity =  float(fuel_oil_type_list["H2_Natural_Gas_info_list"]["ghg_intensity"]["S"])
        sum_ghg += h2_ng * h2_ng_ghg_intensity
        sum_foc += h2_ng

    if sum_foc != 0:
        GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual


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
        lng_oss_lcv =  float(fuel_oil_type_list["LNG_OMS_info_list"]["lcv"]["S"])
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
        nh3_ng_lcv = float(fuel_oil_type_list["NH3_Natural_Gas_info_list"]["lcv"]["S"])
        total_energy += nh3_ng * nh3_ng_lcv
    if nh3_ef > 0:
        nh3_ef_lcv = float(fuel_oil_type_list["NH3_eFuel_info_list"]["lcv"]["S"])
        total_energy += nh3_ef * nh3_ef_lcv
    if methanol_ng > 0:
        methanol_ng_lcv = float(fuel_oil_type_list["Methanol_Natural_Gas_info_list"]["lcv"]["S"])
        total_energy += methanol_ng * methanol_ng_lcv
    if h2_ng > 0:
        h2_ng_lcv = float(fuel_oil_type_list["Methanol_Natural_Gas_info_list"]["lcv"]["S"])
        total_energy += h2_ng * h2_ng_lcv

    return_energy = total_energy

    return return_energy

# コンプライアンスバランスを算出するメソッド
def calc_cb(year_timestamp, energy, GHG_Actual):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    cb = (GHG_Max - GHG_Actual) * energy
    print(f"cb{type(cb)}: {cb}")
    cb_formatted = str(round(float(cb), 1))
    print(f"cb_formatted{type(cb_formatted)}: {cb_formatted}")

    return cb_formatted

# CO2排出量の算出メソッド
def calc_co2(lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng, fuel_oil_type_info_list):

    # EUAの算出
    co2_total   = 0

    if lng_ods > 0:
        lng_ods_co2_factor =  float(fuel_oil_type_info_list["LNG_ODS_info_list"]["emission_factor"]["S"])
        co2_total += lng_ods * lng_ods_co2_factor
    if lng_oms > 0:
        lng_oms_co2_factor =  float(fuel_oil_type_info_list["LNG_OMS_info_list"]["emission_factor"]["S"])
        co2_total += lng_oms * lng_oms_co2_factor
    if lng_oss > 0:
        lng_oss_co2_factor =  float(fuel_oil_type_info_list["LNG_OSS_info_list"]["emission_factor"]["S"])
        co2_total += lng_oms * lng_oss_co2_factor
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
        co2_total = methanol_ng * methanol_ng_co2_factor
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
    print(f"eu_ets_rate: {(eu_ets_rate)}")

    eua       = total_co2 * float(eu_ets_rate) / 100
    print(f"eua{type(eua)}: {eua}")
    return eua