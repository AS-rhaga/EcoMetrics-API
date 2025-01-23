import io
import cgi
import json
import boto3
import base64
import json
from io import StringIO
from datetime import datetime, timedelta

from poseidon import dynamodb

# datachannel;
def util_datachannel(response):
    
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
    
    
def DataChannel(imo_no):
    response = dynamodb.get_data_channel(imo_no)
    response = util_datachannel(response)
    return response