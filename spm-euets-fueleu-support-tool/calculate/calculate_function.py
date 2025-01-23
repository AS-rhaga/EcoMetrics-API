
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
        if int(year) <= 2029:
            target_rate = 2
        elif int(year) <= 2034:
            target_rate = 6
        elif int(year) <= 2039:
            target_rate = 14.5
        elif int(year) <= 2044:
            target_rate = 31
        elif int(year) <= 2049:
            target_rate = 62
        else:
            target_rate = 80
    
        # 削減目標量をもとにGHG強度上限を算出
        GHG_Max = 91.16 * (100- target_rate) / 100

        # borrowing_limitを算出
        borrowing_limit = GHG_Max * 0.02 * energy

    else:
        borrowing_limit = 0

    return borrowing_limit

def calc_GHG_Max(year):
    year = int(year)
    if year <= 2029:
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

def calc_GHG_Actual(total_lng, total_hfo, total_lfo, total_mdo, total_mgo, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list):
    sum_ghg = 0
    sum_foc = 0

    if total_lng > 0:
        lng_ghg_intensity =  float(LNG_info_list["ghg_intensity"]["S"])
        sum_ghg += total_lng * lng_ghg_intensity
        sum_foc += total_lng
    if total_hfo > 0:
        hfo_ghg_intensity =  float(HFO_info_list["ghg_intensity"]["S"])
        sum_ghg += total_hfo * hfo_ghg_intensity
        sum_foc += total_hfo
    if total_lfo > 0:
        lfo_ghg_intensity =  float(LFO_info_list["ghg_intensity"]["S"])
        sum_ghg += total_lfo * lfo_ghg_intensity
        sum_foc += total_lfo
    if total_mdo > 0:
        mdo_ghg_intensity =  float(MDO_info_list["ghg_intensity"]["S"])
        sum_ghg += total_mdo * mdo_ghg_intensity
        sum_foc += total_mdo
    if total_mgo > 0:
        mgo_ghg_intensity =  float(MGO_info_list["ghg_intensity"]["S"])
        sum_ghg += total_mgo * mgo_ghg_intensity
        sum_foc += total_mgo

    GHG_Actual = round(float(sum_ghg / sum_foc), 2)
    print(f"GHG_Actual{type(GHG_Actual)}: {GHG_Actual}")
    return GHG_Actual

# エネルギーの総消費量を算出するメソッド
def calc_energy(eu_rate, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, total_lpg_p, total_ethanol, total_lpg_b, total_methanol, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list):
    total_energy = 0


    if total_lng > 0:
        lng_lcv =  float(LNG_info_list["lcv"]["S"])
        total_energy += total_lng * lng_lcv
    if total_hfo > 0:
        hfo_lcv =  float(HFO_info_list["lcv"]["S"])
        total_energy += total_hfo * hfo_lcv
    if total_lfo > 0:
        lfo_lcv =  float(LFO_info_list["lcv"]["S"])
        total_energy += total_lfo * lfo_lcv
    if total_mdo > 0:
        mdo_lcv =  float(MDO_info_list["lcv"]["S"])
        total_energy += total_mdo * mdo_lcv
    if total_mgo > 0:
        mgo_lcv =  float(MGO_info_list["lcv"]["S"])
        total_energy += total_mgo * mgo_lcv

    return_energy = total_energy * float(eu_rate) / 100

    return return_energy

# コンプライアンスバランスを算出するメソッド
def calc_cb(year_timestamp, energy, GHG_Actual):
    GHG_Max    = calc_GHG_Max(year_timestamp)
    cb = (GHG_Max - GHG_Actual) * energy
    print(f"cb{type(cb)}: {cb}")
    cb_formatted = str(round(float(cb), 1))
    print(f"cb_formatted{type(cb_formatted)}: {cb_formatted}")

    return cb_formatted
