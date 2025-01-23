import json
import ast
from datetime import datetime, timedelta
from decimal import Decimal
# from pyproj import Transformer

from poseidon import dynamodb
from poseidon.Util.VesselMaster import VesselMaster
from poseidon.Util.FuelOilType import FuelOilType
from poseidon.Util import Util


def create_responce_list(imo, FUELOILTYPE, response):
    print(f"response[{type(response)}]: {response}")
    
    VALUE_LIST = []
    RESPONSE_LIST = []
    emission_factor = float(FUELOILTYPE["emission_factor"])
    
    # DynamoDBから取得してきたレコードをリストに移管
    for res in response:
        utc_date            = res["timestamp"]["S"] if 'timestamp' in res and res["timestamp"]["S"] != "" else ""
        state               = res["state"]["S"] if 'state' in res and res["state"]["S"] != "" else ""
        lat                 = float(res["lat"]["S"]) if 'lat' in res and res["lat"]["S"] != "" else ""
        course              = float(res["course"]["S"]) if 'course' in res and res["course"]["S"] != "" else ""
        beaufort            = float(res["beaufort"]["S"]) if 'beaufort' in res and res["beaufort"]["S"] != "" else ""
        # og_distance         = round(float(res["og_distance"]["S"]),1) if 'og_distance' in res and res["og_distance"]["S"] != "" else ""
        # log_speed           = round(float(res["log_speed"]["S"]),1) if 'log_speed' in res and res["log_speed"]["S"] != "" else ""
        # me_rpm              = round(float(res["me_rpm"]["S"]),1) if 'me_rpm' in res and res["me_rpm"]["S"] != "" else ""
        # me_foc              = round(float(res["me_foc"]["S"]),1) if 'me_foc' in res and res["me_foc"]["S"] != "" else ""
        # me_load             = round(float(res["me_load"]["S"]),1) if 'me_load' in res and res["me_load"]["S"] != "" else ""
        # displacement        = round(float(res["displacement"]["S"]),1) if 'displacement' in res and res["displacement"]["S"] != "" else ""
        # wind_speed          = round(float(res["wind_speed"]["S"]),1) if 'wind_speed' in res and res["wind_speed"]["S"] != "" else ""
        # wind_direction      = round(float(res["wind_direction"]["S"]),1) if 'wind_direction' in res and res["wind_direction"]["S"] != "" else ""
        # wave_period         = round(float(res["wave_period"]["S"]),1) if 'wave_period' in res and res["wave_period"]["S"] != "" else ""
        # wave_direction      = round(float(res["wave_direction"]["S"]),1) if 'wave_direction' in res and res["wave_direction"]["S"] != "" else ""
        # wave_height         = round(float(res["wave_height"]["S"]),1) if 'wave_height' in res and res["wave_height"]["S"] != "" else ""
        # swell_height        = round(float(res["swell_height"]["S"]),1) if 'swell_height' in res and res["swell_height"]["S"] != "" else ""
        # swell_period        = round(float(res["swell_period"]["S"]),1) if 'swell_period' in res and res["swell_period"]["S"] != "" else ""
        # swell_direction     = round(float(res["swell_direction"]["S"]),1) if 'swell_direction' in res and res["swell_direction"]["S"] != "" else ""
        
        og_distance         = Util.format_to_one_decimal(round(float(res["og_distance"]["S"]),1)) if 'og_distance' in res and res["og_distance"]["S"] != "" else ""
        log_speed           = Util.format_to_one_decimal(round(float(res["log_speed"]["S"]),1)) if 'log_speed' in res and res["log_speed"]["S"] != "" else ""
        me_rpm              = Util.format_to_one_decimal(round(float(res["me_rpm"]["S"]),1)) if 'me_rpm' in res and res["me_rpm"]["S"] != "" else ""
        me_foc              = Util.format_to_one_decimal(round(float(res["me_foc"]["S"]),1)) if 'me_foc' in res and res["me_foc"]["S"] != "" else ""
        me_load             = Util.format_to_one_decimal(round(float(res["me_load"]["S"]),1)) if 'me_load' in res and res["me_load"]["S"] != "" else ""
        displacement        = Util.format_to_one_decimal(round(float(res["displacement"]["S"]),1)) if 'displacement' in res and res["displacement"]["S"] != "" else ""
        wind_speed          = Util.format_to_one_decimal(round(float(res["wind_speed"]["S"]),1)) if 'wind_speed' in res and res["wind_speed"]["S"] != "" else ""
        wind_direction      = Util.format_to_one_decimal(round(float(res["wind_direction"]["S"]),1)) if 'wind_direction' in res and res["wind_direction"]["S"] != "" else ""
        wave_period         = Util.format_to_one_decimal(round(float(res["wave_period"]["S"]),1)) if 'wave_period' in res and res["wave_period"]["S"] != "" else ""
        wave_direction      = Util.format_to_one_decimal(round(float(res["wave_direction"]["S"]),1)) if 'wave_direction' in res and res["wave_direction"]["S"] != "" else ""
        wave_height         = Util.format_to_one_decimal(round(float(res["wave_height"]["S"]),1)) if 'wave_height' in res and res["wave_height"]["S"] != "" else ""
        swell_height        = Util.format_to_one_decimal(round(float(res["swell_height"]["S"]),1)) if 'swell_height' in res and res["swell_height"]["S"] != "" else ""
        swell_period        = Util.format_to_one_decimal(round(float(res["swell_period"]["S"]),1)) if 'swell_period' in res and res["swell_period"]["S"] != "" else ""
        swell_direction     = Util.format_to_one_decimal(round(float(res["swell_direction"]["S"]),1)) if 'swell_direction' in res and res["swell_direction"]["S"] != "" else ""
        
        print(f"displacement[{type(displacement)}]: {displacement}")
        
        # # lng
        if 'lng' in res and res["lng"]["S"] != "":
            lng = float(res["lng"]["S"])
            lng = lng + 360 if lng < 0 else lng
        else:
            lng = ""
            
        # CO2,foc
        if 'total_foc' in res and res["total_foc"]["S"] != "":
            total_foc = float(res["total_foc"]["S"])
            co2 = round(total_foc * emission_factor, 1)
        else:
            total_foc = ""
            co2 = ""
        
        # -----------------------------------------------------------------------------
        
        
        VALUE_LIST = {
            "utc_date"          : Util.timestamp_for_HighCharts(utc_date),
            "log_speed"         : log_speed,
            "state"             : state,
            "foc"               : total_foc,
            "lat"               : lat,
            "lng"               : lng,
            "co2_factor"        : emission_factor,
            "co2"               : co2,
            "me_foc"            : me_foc,
            "me_load"           : me_load,
            "me_rpm"            : me_rpm,
            "distance"          : og_distance,
            "displacement"      : displacement,
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
        RESPONSE_LIST.append(VALUE_LIST)
    
    return RESPONSE_LIST
    
    
def util_VesselOverview(imo, response):
    
        
    VESSELMASTER = VesselMaster.VesselMaster(imo)
    FUELOILTYPE = FuelOilType.FuelOilType(VESSELMASTER["OilType"])
    RESPONSE_LIST = create_responce_list(imo, FUELOILTYPE, response)
    VesselOverview = {
        "VESSELMASTER": VESSELMASTER,
        "TIMESERIES": RESPONSE_LIST,
    }
    
    return VesselOverview
    
    
def VesselOverview(imo, Timestamp_from, Timestamp_to):
    response = dynamodb.get_noonreport(imo, Timestamp_from, Timestamp_to)
    response = util_VesselOverview(imo, response)
    return response
