
import ast
from datetime import datetime
import json
import math
import re

from dynamodb import select
from calculate import calculate_function

def make_recoed(imo, year, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list):

    # 必要な変数・リストを作成
    last_year             = 0

    operator_list            = []
    thisyear_year_total_list = []
    operator_total_list      = []
    return_datasets           = []

    # VesselMasterを取得
    vessel_master = select.get_vessel_master(imo)

    # imoのみをキーに、year-totalリストを取得
    total_year_total_list = select.get_year_total_by_imo(imo)

    # シミュレーション用テーブルを取得
    select.get_simulation_voyage(imo, year)
    select.get_simulation_speed(imo, year)

    # 
    for year_rec in total_year_total_list:

        if year_rec["year_and_ope"]["S"][0:4] == year:
            thisyear_year_total_list.append(year_rec)

        operator = year_rec["year_and_ope"]["S"][4:50]

        # 現時点ではオペレーターは複数ないので飛ばす
        # if operator not in operator_list : # 上限は適当
            # オペレーター毎に振り分ける
            # operator_list.append(operator)
            # index = operator_list.index(operator)

            # operator_total_list.append([])
        operator_total_list.append(year_rec)

    # 各種燃料の消費量と、消費エネルギーの合計値用変数を設定する。
    total_lng    = 0
    total_hfo    = 0
    total_lfo    = 0
    total_mdo    = 0
    total_mgo    = 0
    total_energy = 0

    # 今年分のyear-totalレコード分ループ
    for rec in thisyear_year_total_list:
        operator = rec["year_and_ope"]["S"][4:50]

        operator_total_list = sorted(operator_total_list, key=lambda x:x["year_and_ope"]["S"], reverse=True)

        # 連続罰金年数カウンターを設定
        consecutive_years = 0

        # 今年borrowingができるかどうかのフラグを設定
        thisYear_borrowing = True

        # 昨年分・今年分のレコードを入れるリスト
        last_year_rec = []
        this_year_rec = []

        for operator_rec in operator_total_list:

            # 罰金フラグの確認
            fine_flag = operator_rec["fine_flag"]["S"]

            if fine_flag == "1":
                consecutive_years += 1
            else:
                consecutive_years = 0
                break

            # 西暦部分の確認
            rec_year = operator_rec["year_and_ope"]["S"][0:4]
            if rec_year == str(int(year - 1)):
                last_year_rec = operator_rec
            elif rec_year == year:
                this_year_rec = operator_rec

        # オペレーター別リストの中に昨年のレコードがあるかを確認する
        last_year = 0
        if len(last_year_rec) != 0:
            last_year_banking   = float(last_year_rec["banking"]["S"])
            last_year_borrowing = float(last_year_rec["borrowing"]["S"])


            if last_year_borrowing > 0:
                last_year += last_year_borrowing * (-1.1)
                thisYear_borrowing = False
            elif last_year_banking > 0:
                last_year += last_year_borrowing
            else:
                last_year += 0

        # オペレーター別リストの中に今年のレコードがあるかを確認する
        if len(this_year_rec) != 0:
            # 各項目を取得する
            operator  = this_year_rec["year_and_ope"]["S"][4:50]
            lng       = float(this_year_rec["total_lng"]["S"])
            hfo       = float(this_year_rec["total_hfo"]["S"])
            lfo       = float(this_year_rec["total_lfo"]["S"])
            mdo       = float(this_year_rec["total_mdo"]["S"])
            mgo       = float(this_year_rec["total_mgo"]["S"])
            distance  = float(this_year_rec["distance"]["S"])
            cb        = float(this_year_rec["cb"]["S"])
            banking   = float(this_year_rec["banking"]["S"])
            borrowing = float(this_year_rec["banking"]["S"])

            # CBから消費量エネルギー（EU Rate考慮済）を算出する
            GHG_Max    = calculate_function.calc_GHG_Max(year)
            GHG_Actual = calculate_function.calc_GHG_Actual(lng, hfo, lfo, mdo, mgo, LNG_info_list, HFO_info_list, LFO_info_list, MDO_info_list, MGO_info_list)
            energy          = cb / (GHG_Max - GHG_Actual)

            # 必要な計算を行う
            foc             = lng + hfo + lfo + mdo + mgo
            total_cb        = cb + borrowing + banking + last_year
            borrowing_limit = calculate_function.calc_borrowing_limit(thisYear_borrowing, year, energy)
            penalty_factor  = (consecutive_years - 1) / 10 + 1

            if cb < 0:
                cost = abs(cb) * 2400 / (GHG_Actual * 41000)

            dataset = {
                "imo"            : imo,
                "operator"       : operator,
                "distance"       : distance,
                "foc"            : foc,
                "year_to_date"   : total_cb,
                "last_year"      : last_year,
                "borrowing_limit": borrowing_limit,
                "borrowing"      : borrowing,
                "banking"        : banking,
                "total"          : total_cb + last_year,
                "penalty_factor" : penalty_factor,
                "cost"           : cost
            }

            return_datasets.append(dataset)

            # 合計用変数に加算する。
            total_lng    += lng
            total_hfo    += hfo
            total_lfo    += lfo
            total_mdo    += mdo
            total_mgo    += mgo
            total_energy += energy

    
    return return_datasets, total_lng, total_hfo, total_lfo, total_mdo, total_mgo, total_energy
