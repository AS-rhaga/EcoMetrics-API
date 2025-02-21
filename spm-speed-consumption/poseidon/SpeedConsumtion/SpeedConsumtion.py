import json
import ast
from datetime import datetime, timedelta

from poseidon import dynamodb
from poseidon.Util.VesselMaster import VesselMaster
from poseidon.Util.FuelOilType import FuelOilType
from poseidon.Util import Util


Trendline_color = "#D8FFF8"
CPCurve_color = "#F16060"
ReferenceCurve_color = "#CAA138"
SpeedConsumtionCurve1_color = "#4CAA3C"
SpeedConsumtionCurve2_color = "#00b0ff"
SpeedConsumtionCurve3_color = "#d4a8ea"
SpeedConsumtionCurve4_color = "#d500f9"

# 事前に登録したスピコンカーブを取得
def create_registered_spcon_curve(sp):
    
    color_list = [CPCurve_color, ReferenceCurve_color, SpeedConsumtionCurve1_color, SpeedConsumtionCurve2_color, SpeedConsumtionCurve3_color, SpeedConsumtionCurve4_color] 
    res_sp_list = []
    for i in range(len(sp)):
        
        # 任意項目の設定値有無判定
        if 'description' in sp[i]:
            temp_description = sp[i]["description"]["S"]
        else:
            temp_description = ""
        
        sp_dict = {
            "alpha"     : float(sp[i]["alpha"]["S"]),
            "a"         : float(sp[i]["a"]["S"]),
            "C"         : float(sp[i]["C"]["S"]),
            "color"     : color_list[i],
            "name"      : sp[i]["name"]["S"],
            "display"   : sp[i]["display"]["S"],
            "description"   : temp_description,
        }
        res_sp_list.append(sp_dict)
    
    registered_sp = []
    for sp in res_sp_list:
        print(f"sp: {sp}")
        if sp["display"] == "1":
            
            print(f"事前登録スピコンカーブ: FOC = {sp["alpha"]} × X^{sp["a"]} + {sp["C"]}")
            formula = {
                "alpha" :sp["alpha"],
                "a"     :sp["a"],
                "C"     :sp["C"]
            }
            
            sp_list = []
            for x in range(45):
                # y = sp["alpha"] * (1 * pow(x, sp["a"]) + sp["C"])
                y = sp["alpha"] * pow(x, sp["a"]) + sp["C"]
                sp_list.append([x, y])
                
            sp_dict = {}
            sp_dict["data"] = sp_list
            sp_dict["color"] = sp["color"]
            sp_dict["name"] = sp["name"]
            sp_dict["formula"] = formula
            sp_dict["description"] = sp["description"]
            
            registered_sp.append(sp_dict)
        
        elif sp["display"] == "0":
            None
    
    registered_sp_list = []
    for index in range(len(registered_sp)):
        dataSet = {
            "name": registered_sp[index]["name"], 
            "data": registered_sp[index]["data"], 
            "color": registered_sp[index]["color"], 
            "checked": True,
            "formula":registered_sp[index]["formula"],
            "description": registered_sp[index]["description"]
        }
        registered_sp_list.append(dataSet)
    
    return registered_sp_list



# NoonReportデータからスピコン傾向線を算出
def create_tendency_spcon_curve(SPEED_LIST, FOC_LIST, DISPLACEMENT_LIST, B_Disp):
    
    sp_list = []
    
    # MTI/NYK計算式------------------------------------------------------------------------------------------------------------------------------------
    # スピコンカーブ(船速V,燃料消費量FOCの関係)は、FOC=α(a×V^A+C) で表現する。
    # ここでαは経年劣化係数（船齢に応じて1.00~1.10程度）、Aは通常３や2.8といった値をとる。Cの値は通常0とする。
    A = 3.00
    C = 0
    alpha = 1.0
    B = 0.4
    # 1)	データ抽出後にLEG(All/Ballast/Laden)、OG/LOG毎に、データ系列としてまとめる
    # 傾向線のため必要に応じて画面側でフィルタをかけてもらう。

    # 2)    1)でまとめたデータ系列毎に排水量の平均値を計算し、補正排水量B_Dispとする
    B_Disp = B_Disp
    # print(f"B_Disp: {B_Disp}")

    a_m1_n_sigma = 0
    for n in range(len(DISPLACEMENT_LIST)):
        # 3)    補正排水量B_Dispを用いて補正船速〖Vc〗_nをもとめる*1
        V_cn = SPEED_LIST[n] * pow((DISPLACEMENT_LIST[n] / B_Disp), B / A)
        # print(f"V_cn: {V_cn}")
    
        # 4)	個々のデータについてカーブ係数の逆数a_n^(-1)をもとめる
        a_m1_n = pow(V_cn, A) / ((FOC_LIST[n] / alpha) - C)
        # print(f"a_m1_n: {a_m1_n}")
        
        a_m1_n_sigma += a_m1_n
    # print(f"a_m1_n_sigma: {a_m1_n_sigma}")
    
    # 5)	a_n^(-1)の平均値をもとめ、この逆数をスピコンカーブの係数aとする
    a =  len(DISPLACEMENT_LIST) / (a_m1_n_sigma)
    # print(f"a: {a}")
    
    for x in range(45):
        y = alpha * (a * pow(x, A) + C)
        sp_list.append([x, y])
    
    print(f"傾向スピコンカーブ[変換前]: FOC = {alpha}({a}×V^{A}+{C})")
    # --------------------------------------------------------------------------------------------------------------------------------------------
    
    print(f"sp_list: {sp_list}")
    sp_dict = {}
    sp_dict["name"] = 'TrendLine'
    sp_dict["data"] = sp_list
    sp_dict["color"] = Trendline_color
    sp_dict["checked"] = True
    sp_dict["formula"] = {
        "alpha" :alpha,
        "a"     :round(a, 3),
        "C"     :C
    }
    
    print(f"sp_dict: {sp_dict}")
    
    return sp_dict


# NoonReportデータを取得
def data_formatting(imo, response, fromDisp, toDisp, BallastLaden, fromLogSpeed, toLogSpeed, fromEngineLoad, toEngineLoad, Beaufort, fromLogSpeedAF, toLogSpeedAF, fromFOCAF, toFOCAF, VESSELMASTER, fuel_oil_type):
    
    TIMESERIES = []
    SPEED_LIST = []
    FOC_LIST = []
    DISPLACEMENT_LIST = []
    B_Disp = 0
    Defaultupperlimit = 99999999
    
    # Ballast/Ladenのチェックボックスによるフィルターをセット
    emission_factor = float(fuel_oil_type["emission_factor"])
    Ballast = float(VESSELMASTER["Ballast"])
    Laden = float(VESSELMASTER["Laden"])
    min_disp = 0
    max_disp = Defaultupperlimit
    if BallastLaden == "Ballast":
        max_disp = Ballast
    elif BallastLaden == "Laden":
        min_disp = Ballast
        max_disp = Laden
    elif BallastLaden == "All":
        None
    else:
        None
    
    # Displacement、LogSpeed、M/ELoadのフィルターをセット
    fromDisp = round(float(fromDisp), 2) if fromDisp != '' else 0
    toDisp = round(float(toDisp), 2) if toDisp != '' else Defaultupperlimit
    fromLogSpeed = round(float(fromLogSpeed), 2) if fromLogSpeed != '' else 0
    toLogSpeed = round(float(toLogSpeed), 2) if toLogSpeed != '' else Defaultupperlimit
    fromEngineLoad = round(float(fromEngineLoad), 2) if fromEngineLoad != '' else 0
    toEngineLoad = round(float(toEngineLoad), 2) if toEngineLoad != '' else Defaultupperlimit
    
    print(f"min_disp[{type(min_disp)}]: {min_disp}, max_disp[{type(max_disp)}]: {max_disp}")
    print(f"fromDisp[{type(fromDisp)}]: {fromDisp},toDisp[{type(toDisp)}]: {toDisp}")
    print(f"fromLogSpeed[{type(fromLogSpeed)}]: {fromLogSpeed}, toLogSpeed[{type(toLogSpeed)}]: {toLogSpeed}")
    print(f"fromEngineLoad[{type(fromEngineLoad)}]: {fromEngineLoad}, toEngineLoad[{type(toEngineLoad)}]: {toEngineLoad}")
    
    # Advanced Filterをセット
    # フィルターリストを作成する
    fromLogSpeedAFsplitted = fromLogSpeedAF.split("-")
    toLogSpeedAFsplitted   = toLogSpeedAF.split("-")
    fromFOCAFsplitted      = fromFOCAF.split("-")
    toFOCAFsplitted        = toFOCAF.split("-")
    fromLogSpeedAFList = []
    toLogSpeedAFList   = []
    fromFOCAFList      = []
    toFOCAFList        = []
    i = 0
    for i in range(len(fromLogSpeedAFsplitted)):

        # LogSpeed, FOCフィルターの上限下限が全て未入力の場合(LogSpeed:True,FOC:Trueにする)
        if fromLogSpeedAFsplitted[i] == "" and toLogSpeedAFsplitted[i] == "" and fromFOCAFsplitted[i] == "" and toFOCAFsplitted[i] == "":
            fromLogSpeedAFList.append(Defaultupperlimit)
            toLogSpeedAFList.append(0)
            fromFOCAFList.append(Defaultupperlimit)
            toFOCAFList.append(0)
            continue

        # LogSpeedフィルターの確認
        # LogSpeedフィルターの上限下限が両方未入力、FOCフィルターには一つ以上入力ありの場合、
        # FOCフィルターの指定範囲がFalse,Falseになるよう、LogSpeed側では全範囲をFalseにする。
        if fromLogSpeedAFsplitted[i] == "" and toLogSpeedAFsplitted[i] == "":
            fromLogSpeedAFList.append(0)
            toLogSpeedAFList.append(Defaultupperlimit)
        # LogSpeedフィルターの下限が未入力の場合
        elif fromLogSpeedAFsplitted[i] == "":
            fromLogSpeedAFList.append(0)
            toLogSpeedAFList.append(round(float(toLogSpeedAFsplitted[i]), 2))
        # LogSpeedフィルターの上限が未入力の場合
        elif toLogSpeedAFsplitted[i] == "":
            fromLogSpeedAFList.append(round(float(fromLogSpeedAFsplitted[i]), 2))
            toLogSpeedAFList.append(Defaultupperlimit)
        # LogSpeedフィルターの上限下限が両方入力されている場合
        else:
            fromLogSpeedAFList.append(round(float(fromLogSpeedAFsplitted[i]), 2))
            toLogSpeedAFList.append(round(float(toLogSpeedAFsplitted[i]), 2))

        # FOCフィルターの確認
        # FOCフィルターの上限下限が両方未入力、LogSpeedフィルターには一つ以上入力ありの場合、
        # LogSpeedフィルターの指定範囲がFalse,Falseになるよう、FOC側では全範囲をFalseにする。
        if fromFOCAFsplitted[i] == "" and toFOCAFsplitted[i] == "":
            fromFOCAFList.append(0)
            toFOCAFList.append(Defaultupperlimit)
        # FOCフィルターの下限が未入力の場合
        elif fromFOCAFsplitted[i] == "":
            fromFOCAFList.append(0)
            toFOCAFList.append(round(float(toFOCAFsplitted[i]), 2))
        # FOCフィルターの上限が未入力の場合
        elif toFOCAFsplitted[i] == "":
            fromFOCAFList.append(round(float(fromFOCAFsplitted[i]), 2))
            toFOCAFList.append(Defaultupperlimit)
        # FOCフィルターの上限下限が両方入力されている場合
        else:
            fromFOCAFList.append(round(float(fromFOCAFsplitted[i]), 2))
            toFOCAFList.append(round(float(toFOCAFsplitted[i]), 2))

    print(f"fromLogSpeedAFsplitted[{type(fromLogSpeedAFsplitted)}]: {fromLogSpeedAFsplitted}, toLogSpeedAFsplitted[{type(toLogSpeedAFsplitted)}]: {toLogSpeedAFsplitted}")
    print(f"fromFOCAFsplitted[{type(fromFOCAFsplitted)}]: {fromFOCAFsplitted}, toFOCAFsplitted[{type(toFOCAFsplitted)}]: {toFOCAFsplitted}")
    print(f"fromLogSpeedAFList[{type(fromLogSpeedAFList)}]: {fromLogSpeedAFList}, toLogSpeedAFList[{type(toLogSpeedAFList)}]: {toLogSpeedAFList}")
    print(f"fromFOCAFList[{type(fromFOCAFList)}]: {fromFOCAFList}, toFOCAFList[{type(toFOCAFList)}]: {toFOCAFList}")
    
    def checkAdvancedFilters(log_speed_float, me_foc_float):

        print(f"[checkAdvancedFilters]log_speed_float:{(log_speed_float)}, me_foc_float:{(me_foc_float)})")

        result = False
        i = 0

        # LogSpeedAFチェック
        def checkLogSpeedAF(i):
            # log_speed_floatが入力範囲外の場合
            if (log_speed_float <= fromLogSpeedAFList[i] or toLogSpeedAFList[i] <= log_speed_float):
                scResult = True
            else:
                scResult = False
            return scResult
        # FOCAFチェック
        def checkFOCAF(i):
            # me_foc_floatが入力範囲外の場合
            if (me_foc_float <= fromFOCAFList[i] or toFOCAFList[i] <= me_foc_float):
                focResult = True
            else:
                focResult = False
            return focResult

        for i in range(len(fromLogSpeedAFList)):
            print(f"fromLogSpeedAFList[{i}]: {fromLogSpeedAFList[i]}, toLogSpeedAFList[{i}]: {toLogSpeedAFList[i]}, fromFOCAFList[{i}]: {fromFOCAFList[i]}, toFOCAFList[{i}]: {toFOCAFList[i]}")
            if checkLogSpeedAF(i) == False and checkFOCAF(i) == False:
                result = False
                print(f"checkAdvancedFilters{i}: {result}（checkLogSpeedAF{i}:{checkLogSpeedAF(i)}, checkFOCAFAF{i}:{checkFOCAF(i)}）")
                break
            else:
                result = True
                print(f"checkAdvancedFilters{i}: {result}（checkLogSpeedAF{i}:{checkLogSpeedAF(i)}, checkFOCAFAF{i}:{checkFOCAF(i)}）")
                i += 1
        return result

    beaufort_filter = []
    for b in Beaufort.split("-"):
        # Beaufort空白の場合も除外するなら処理を「None」に変更
        if b == "":
            beaufort_filter.append("")
        else:
            beaufort_filter.append(int(b))
        
    total_count = 0
    valid_count = 0
    for res in response:
        total_count += 1
        
        utc_date            = res["timestamp"]["S"] if 'timestamp' in res and res["timestamp"]["S"] != "" else ""
        course              = float(res["course"]["S"]) if 'course' in res and res["course"]["S"] != "" else ""
        beaufort            = float(res["beaufort"]["S"]) if 'beaufort' in res and res["beaufort"]["S"] != "" else ""
        og_distance         = Util.format_to_one_decimal(round(float(res["og_distance"]["S"]), 1)) if 'og_distance' in res and res["og_distance"]["S"] != "" else ""
        log_speed           = Util.format_to_one_decimal(round(float(res["log_speed"]["S"]), 1)) if 'log_speed' in res and res["log_speed"]["S"] != "" else ""
        me_rpm              = Util.format_to_one_decimal(round(float(res["me_rpm"]["S"]), 1)) if 'me_rpm' in res and res["me_rpm"]["S"] != "" else ""
        me_foc              = Util.format_to_one_decimal(round(float(res["me_foc"]["S"]), 1)) if 'me_foc' in res and res["me_foc"]["S"] != "" else ""
        me_load             = Util.format_to_one_decimal(round(float(res["me_load"]["S"]), 1)) if 'me_load' in res and res["me_load"]["S"] != "" else ""
        displacement        = Util.format_to_one_decimal(round(float(res["displacement"]["S"]), 1)) if 'displacement' in res and res["displacement"]["S"] != "" else ""
        wind_speed          = Util.format_to_one_decimal(round(float(res["wind_speed"]["S"]), 1)) if 'wind_speed' in res and res["wind_speed"]["S"] != "" else ""
        wind_direction      = Util.format_to_one_decimal(round(float(res["wind_direction"]["S"]), 1)) if 'wind_direction' in res and res["wind_direction"]["S"] != "" else ""
        wave_period         = Util.format_to_one_decimal(round(float(res["wave_period"]["S"]), 1)) if 'wave_period' in res and res["wave_period"]["S"] != "" else ""
        wave_direction      = Util.format_to_one_decimal(round(float(res["wave_direction"]["S"]), 1)) if 'wave_direction' in res and res["wave_direction"]["S"] != "" else ""
        wave_height         = Util.format_to_one_decimal(round(float(res["wave_height"]["S"]), 1)) if 'wave_height' in res and res["wave_height"]["S"] != "" else ""
        swell_height        = Util.format_to_one_decimal(round(float(res["swell_height"]["S"]), 1)) if 'swell_height' in res and res["swell_height"]["S"] != "" else ""
        swell_period        = Util.format_to_one_decimal(round(float(res["swell_period"]["S"]), 1)) if 'swell_period' in res and res["swell_period"]["S"] != "" else ""
        swell_direction     = Util.format_to_one_decimal(round(float(res["swell_direction"]["S"]), 1)) if 'swell_direction' in res and res["swell_direction"]["S"] != "" else ""
        
        
        log_speed_float     = float(res["log_speed"]["S"]) if 'log_speed' in res and res["log_speed"]["S"] != "" else ""
        me_foc_float        = float(res["me_foc"]["S"]) if 'me_foc' in res and res["me_foc"]["S"] != "" else ""
        me_load_float       = float(res["me_load"]["S"]) if 'me_load' in res and res["me_load"]["S"] != "" else ""
        displacement_float  = float(res["displacement"]["S"]) if 'displacement' in res and res["displacement"]["S"] != "" else ""
            
        print(f"displacement[{type(displacement)}]: {displacement}")
        # CO2,foc
        if 'total_foc' in res and res["total_foc"]["S"] != "":
            total_foc = float(res["total_foc"]["S"])
            co2 = Util.format_to_one_decimal(round(total_foc * emission_factor, 1))
        else:
            total_foc = ""
            co2 = ""
            
        if 'displacement' in res and 'log_speed' in res and 'me_load' in res and 'total_foc' in res and 'me_foc' in res and 'me_load' in res and displacement != "" and log_speed != "" and me_load != "" and total_foc != "" and me_foc != "" and me_load != "":
            if min_disp <= displacement_float and displacement_float <= max_disp and fromDisp <= displacement_float and displacement_float <= toDisp and fromLogSpeed <= log_speed_float and log_speed_float <= toLogSpeed and fromEngineLoad <= me_load_float and me_load_float <= toEngineLoad:
                if checkAdvancedFilters(log_speed_float, me_foc_float):
                    print(f"checkAdvancedFilters: True")
                    if beaufort in beaufort_filter:
                        VALUE_LIST = {
                            "utc_date"          : utc_date,
                            "co2_factor"        : emission_factor,
                            "co2"               : co2,
                            "foc"               : total_foc,
                            "me_foc"            : me_foc,
                            "me_load"           : me_load,
                            "log_speed"         : log_speed,
                            "me_rpm"            : me_rpm,
                            "distance"          : og_distance,
                            "displacement"      : round(displacement_float),
                            "wind_direction"    : wind_direction,
                            "wind_speed"        : wind_speed,
                            "wave_direction"    : wave_direction,
                            "wave_height"       : wave_height,
                            "wave_period"       : wave_period,
                            "swell_direction"   : swell_direction,
                            "swell_height"      : swell_height,
                            "swell_period"      : swell_period,
                            "course"            : course,
                            "beaufort"          : beaufort,
                        }
                        TIMESERIES.append(VALUE_LIST)
                        
                        FOC_LIST.append(me_foc_float)
                        SPEED_LIST.append(log_speed_float)
                        DISPLACEMENT_LIST.append(displacement_float)
                        B_Disp += displacement_float
                        valid_count += 1
                        # print(f"me_foc: {me_foc}, log_speed: {log_speed}")
                        print(f"{utc_date},{me_foc}, {log_speed}, {displacement}")
        
    
    print(f"len(DISPLACEMENT_LIST): {len(DISPLACEMENT_LIST)}")
    print(f"valid_count: {valid_count}")
    print(f"total_count: {total_count}")
    B_Disp = B_Disp / len(DISPLACEMENT_LIST) if len(TIMESERIES) else None

    response = {
        "timeseries": TIMESERIES,
        "speed_list": SPEED_LIST,
        "foc_list": FOC_LIST,
        "displacement_list": DISPLACEMENT_LIST,
        "B_Disp": B_Disp,
    }
    print(f"response[{type(response)}]: {response}")
    
    return response
    
    
    
# 各種データ算出メソッドを呼び出し、フロントエンド用データセットにまとめて返却
def util_SpeedConsumtion(imo, Timestamp_from, Timestamp_to, fromDisp, toDisp, BallastLaden, fromLogSpeed, toLogSpeed, fromEngineLoad, toEngineLoad, Beaufort, fromLogSpeedAF, toLogSpeedAF, fromFOCAF, toFOCAF):
    
    VESSELMASTER = VesselMaster.VesselMaster(imo)
    fuel_oil_type = FuelOilType.FuelOilType(VESSELMASTER["OilType"])
    
    # NoonReportデータを取得
    res_np = dynamodb.get_noonreport(imo, Timestamp_from, Timestamp_to)
    response = data_formatting(imo, res_np, fromDisp, toDisp, BallastLaden, fromLogSpeed, toLogSpeed, fromEngineLoad, toEngineLoad, Beaufort, fromLogSpeedAF, toLogSpeedAF, fromFOCAF, toFOCAF, VESSELMASTER, fuel_oil_type)
    
    # 事前に登録したスピコンカーブを取得
    speed_consumption_curve = dynamodb.get_speed_consumption_curve(imo)
    registered_spcon_curve = create_registered_spcon_curve(speed_consumption_curve)
    
    # NoonReportデータからスピコン傾向線を算出
    tendency_spcon_curve = create_tendency_spcon_curve(response["speed_list"], response["foc_list"], response["displacement_list"], response["B_Disp"]) if len(response["timeseries"]) else {"name":"", "data":[], "color": ""}
       
    
    SpeedConsumtion = {
        "VESSELMASTER": VESSELMASTER,
        "TIMESERIES": response["timeseries"],
        "SPEEDCONSUMTION": registered_spcon_curve,
        "SPEEDCONSUMTIONTRENDLINE": tendency_spcon_curve,
    }
    
    print(f"SpeedConsumtion[{type(SpeedConsumtion)}]: {SpeedConsumtion}")
    
    return SpeedConsumtion
    
    
def SpeedConsumtion(imo, Timestamp_from, Timestamp_to, fromDisp, toDisp, BallastLaden, fromLogSpeed, toLogSpeed, fromEngineLoad, toEngineLoad, Beaufort, fromLogSpeedAF, toLogSpeedAF, fromFOCAF, toFOCAF):
    response = util_SpeedConsumtion(imo, Timestamp_from, Timestamp_to, fromDisp, toDisp, BallastLaden, fromLogSpeed, toLogSpeed, fromEngineLoad, toEngineLoad, Beaufort, fromLogSpeedAF, toLogSpeedAF, fromFOCAF, toFOCAF)
    return response
