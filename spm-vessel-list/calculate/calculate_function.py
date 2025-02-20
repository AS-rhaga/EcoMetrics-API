
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

# EU Rate考慮済のco2排出量を算出する
def calc_co2(lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng, fuel_oil_type_list):
    
    # 合計用の変数を設定
    total_co2 = 0
    
    if lng_ods > 0:
        lng_ods_co2_factor =  float(fuel_oil_type_list["LNG_ODS_info_list"]["emission_factor"]["S"])
        total_co2 += lng_ods * lng_ods_co2_factor
    if lng_oms > 0:
        lng_oms_co2_factor =  float(fuel_oil_type_list["LNG_OMS_info_list"]["emission_factor"]["S"])
        total_co2 += lng_oms * lng_oms_co2_factor
    if lng_oss > 0:
        lng_oss_co2_factor =  float(fuel_oil_type_list["LNG_OSS_info_list"]["emission_factor"]["S"])
        total_co2 += lng_oss * lng_oss_co2_factor
    if hfo > 0:
        hfo_co2_factor =  float(fuel_oil_type_list["HFO_info_list"]["emission_factor"]["S"])
        total_co2 += hfo * hfo_co2_factor
    if lfo > 0:
        lfo_co2_factor =  float(fuel_oil_type_list["LFO_info_list"]["emission_factor"]["S"])
        total_co2 += lfo * lfo_co2_factor
    if mdo > 0:
        mdo_co2_factor =  float(fuel_oil_type_list["MDO_info_list"]["emission_factor"]["S"])
        total_co2 += mdo * mdo_co2_factor
    if mgo > 0:
        mgo_co2_factor =  float(fuel_oil_type_list["MGO_info_list"]["emission_factor"]["S"])
        total_co2 += mgo * mgo_co2_factor
    if lpg_p > 0:
        lpg_p_co2_factor =  float(fuel_oil_type_list["LPG_Puropane_info_list"]["emission_factor"]["S"])
        total_co2 += lpg_p * lpg_p_co2_factor
    if lpg_b > 0:
        lpg_b_co2_factor =  float(fuel_oil_type_list["LPG_Butane_info_list"]["emission_factor"]["S"])
        total_co2 += lpg_b * lpg_b_co2_factor
    if nh3_ng > 0:
        nh3_ng_co2_factor =  float(fuel_oil_type_list["NH3_Ng_info_list"]["emission_factor"]["S"])
        total_co2 += nh3_ng * nh3_ng_co2_factor
    if nh3_ef > 0:
        nh3_ef_co2_factor =  float(fuel_oil_type_list["NH3_eFuel_info_list"]["emission_factor"]["S"])
        total_co2 += nh3_ef * nh3_ef_co2_factor
    if methanol_ng > 0:
        methanol_ng_co2_factor =  float(fuel_oil_type_list["Methanol_Ng_info_list"]["emission_factor"]["S"])
        total_co2 += methanol_ng * methanol_ng_co2_factor
    if h2_ng > 0:
        h2_ng_co2_factor =  float(fuel_oil_type_list["H2_Ng_info_list"]["emission_factor"]["S"])
        total_co2 += h2_ng * h2_ng_co2_factor

    # CO2の総排出量(MT)
    co2       = total_co2

    return co2

# EUAを算出する 
def calc_EUA(year, lng_ods, lng_oms, lng_oss, hfo, lfo, mdo, mgo, lpg_p, lpg_b, nh3_ng, nh3_ef, methanol_ng, h2_ng, fuel_oil_type_list):

    # EUAの算出
    total_co2 = 0
    eu_ets_rate = 0
    eua = 0

    # EU-ETS対象割合を確認
    if year == "2024":
        eu_ets_rate = 40
    elif year == "2025":
        eu_ets_rate = 70
    else:
        eu_ets_rate = 100

    if lng_ods > 0:
        lng_ods_co2_factor =  float(fuel_oil_type_list["LNG_ODS_info_list"]["emission_factor"]["S"])
        total_co2 += lng_ods * lng_ods_co2_factor
    if lng_oms > 0:
        lng_oms_co2_factor =  float(fuel_oil_type_list["LNG_OMS_info_list"]["emission_factor"]["S"])
        total_co2 += lng_oms * lng_oms_co2_factor
    if lng_oss > 0:
        lng_oss_co2_factor =  float(fuel_oil_type_list["LNG_OSS_info_list"]["emission_factor"]["S"])
        total_co2 += lng_oss * lng_oss_co2_factor
    if hfo > 0:
        hfo_co2_factor =  float(fuel_oil_type_list["HFO_info_list"]["emission_factor"]["S"])
        total_co2 += hfo * hfo_co2_factor
    if lfo > 0:
        lfo_co2_factor =  float(fuel_oil_type_list["LFO_info_list"]["emission_factor"]["S"])
        total_co2 += lfo * lfo_co2_factor
    if mdo > 0:
        mdo_co2_factor =  float(fuel_oil_type_list["MDO_info_list"]["emission_factor"]["S"])
        total_co2 += mdo * mdo_co2_factor
    if mgo > 0:
        mgo_co2_factor =  float(fuel_oil_type_list["MGO_info_list"]["emission_factor"]["S"])
        total_co2 += mgo * mgo_co2_factor
    if lpg_p > 0:
        lpg_p_co2_factor =  float(fuel_oil_type_list["LPG_Puropane_info_list"]["emission_factor"]["S"])
        total_co2 += lpg_p * lpg_p_co2_factor
    if lpg_b > 0:
        lpg_b_co2_factor =  float(fuel_oil_type_list["LPG_Butane_info_list"]["emission_factor"]["S"])
        total_co2 += lpg_b * lpg_b_co2_factor
    if nh3_ng > 0:
        nh3_ng_co2_factor =  float(fuel_oil_type_list["NH3_Ng_info_list"]["emission_factor"]["S"])
        total_co2 += nh3_ng * nh3_ng_co2_factor
    if nh3_ef > 0:
        nh3_ef_co2_factor =  float(fuel_oil_type_list["NH3_eFuel_info_list"]["emission_factor"]["S"])
        total_co2 += nh3_ef * nh3_ef_co2_factor
    if methanol_ng > 0:
        methanol_ng_co2_factor =  float(fuel_oil_type_list["Methanol_Ng_info_list"]["emission_factor"]["S"])
        total_co2 += methanol_ng * methanol_ng_co2_factor
    if h2_ng > 0:
        h2_ng_co2_factor =  float(fuel_oil_type_list["H2_Ng_info_list"]["emission_factor"]["S"])
        total_co2 += h2_ng * h2_ng_co2_factor

    # CO2の総排出量(MT)
    # print(f"total_co2{type(total_co2)}: {total_co2}")
    eua       = total_co2 * float(eu_ets_rate) / 100
    # eua_formatted = str(round(float(eua), 1))
    # print(f"eua_formatted{type(eua_formatted)}: {eua_formatted}")

    return eua

def calc_GHG_Max(year):
    year = float(year)
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

    GHG_Actual = 0
    if sum_foc > 0:
        GHG_Actual = round(float(sum_ghg / sum_foc), 2)
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
def calc_cb(energy, GHG_Actual, GHG_Max):
    cb = (GHG_Max - GHG_Actual) * energy
    print(f"cb{type(cb)}: {cb}")
    # cb_formatted = str(round(float(cb), 1))
    # print(f"cb_formatted{type(cb_formatted)}: {cb_formatted}")

    return cb

# CII算出メソッド
def calc_cii(co2, distance, cii_ref, cii_rating, cii_reduction_rate, VESSELMASTER):
    
    CII_Calculated = 0
    CII_Score      = ""

    # ゼロ割を回避するため
    if distance > 0:
    
        dwt         = float(VESSELMASTER[0]["Deadweight"]["S"])
        gt          = float(VESSELMASTER[0]["Grosstongue"]["S"])   
        
        # リファレンスライン取得------------------------------------------
        weight = 0
        if cii_ref[0]["weight"]["S"] == "DWT":
            weight = dwt
        elif cii_ref[0]["weight"]["S"] == "GT":
            weight = gt
        elif cii_ref[0]["weight"]["S"] == "0":
            weight = 0
            
        less        = cii_ref[0]["less"]["S"]
        less_more   = cii_ref[0]["less_more"]["S"]
        more        = cii_ref[0]["more"]["S"]
        less_value  = float(cii_ref[0]["less_value"]["S"])
        more_value  = float(cii_ref[0]["more_value"]["S"])
        less_a      = float(cii_ref[0]["less_a"]["S"])
        less_c      = float(cii_ref[0]["less_c"]["S"])
        less_more_a = float(cii_ref[0]["less_more_a"]["S"])
        less_more_c = float(cii_ref[0]["less_more_c"]["S"])
        more_a      = float(cii_ref[0]["more_a"]["S"])
        more_c      = float(cii_ref[0]["more_c"]["S"])
        
        a_G2 = 0
        c_G2 = 0
        if less == "1" and less_more == "1" and more == "1":
            if weight < less_value:
                a_G2 = less_a
                c_G2 = less_c
            elif less_value <= weight and weight < more_value:
                a_G2 = less_more_a
                c_G2 = less_more_c
            elif more_value <= weight:
                a_G2 = more_a
                c_G2 = more_c
        elif less == "1" and less_more == "0" and more == "1":
            if weight < less_value:
                a_G2 = less_a
                c_G2 = less_c
            elif more_value <= weight:
                a_G2 = more_a
                c_G2 = more_c
        elif less == "0" and less_more == "0" and more == "0":
            a_G2 = less_a
            c_G2 = less_c
        
        # レーティング取得------------------------------------------
        weight_rating = 0
        weight_type = cii_rating[0]["weight_type"]["S"]
        if weight_type == "DWT":
            weight_rating = dwt
        elif weight_type == "GT":
            weight_rating = gt
        elif weight_type == "0":
            weight_rating = 0
        
        if weight_rating != 0:
            weight_value = float(cii_rating[0]["weight_value"]["S"])
            if weight_rating < weight_value:
                rating_1 = float(cii_rating[0]["less_d1"]["S"])
                rating_2 = float(cii_rating[0]["less_d2"]["S"])
                rating_3 = float(cii_rating[0]["less_d3"]["S"])
                rating_4 = float(cii_rating[0]["less_d4"]["S"])
            elif weight_value <= weight_rating:
                rating_1 = float(cii_rating[0]["more_d1"]["S"])
                rating_2 = float(cii_rating[0]["more_d2"]["S"])
                rating_3 = float(cii_rating[0]["more_d3"]["S"])
                rating_4 = float(cii_rating[0]["more_d4"]["S"])
        elif weight_rating == 0:
            rating_1 = float(cii_rating[0]["less_d1"]["S"])
            rating_2 = float(cii_rating[0]["less_d2"]["S"])
            rating_3 = float(cii_rating[0]["less_d3"]["S"])
            rating_4 = float(cii_rating[0]["less_d4"]["S"])
            
        # 削減率セット
        reduction_rate = float(cii_reduction_rate[0]["reduction_rate"]["S"])
        
        # CII計算
        CII_Attained        = (co2 * pow(10, 6)) / (dwt * distance)                 # Attained CII(G1)
        CII_Reference = a_G2 * pow(dwt, (-1 * c_G2))                          # CII ref. （G2）
        CII_Required        = CII_Reference * ((100 - reduction_rate) / 100)  # Required CII （G3, 2023）
        CII_Calculated      = CII_Attained / CII_Required                          # Attained CII / Required CII
        
        # CII計算値からCIIスコアを算出
        CII_Score = ""
        if CII_Calculated < rating_1:
            CII_Score = "A"
        elif rating_1 <= CII_Calculated and CII_Calculated < rating_2:
            CII_Score = "B"
        elif rating_2 <= CII_Calculated and CII_Calculated < rating_3:
            CII_Score = "C"
        elif rating_3 <= CII_Calculated and CII_Calculated < rating_4:
            CII_Score = "D"
        elif rating_4 < CII_Calculated:
            CII_Score = "E"
            
    return CII_Calculated, CII_Score
