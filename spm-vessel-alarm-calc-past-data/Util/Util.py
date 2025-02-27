from datetime import datetime, timedelta


def timestamp_Z_datetime(timestamp):
    
    try:
        timestamp = timestamp.rstrip('Z')
        timestamp = datetime.fromisoformat(timestamp)
        
        return timestamp
        
    except Exception as e:
        print(f"e: {e.args}")
        return ""
    
    return timestamp