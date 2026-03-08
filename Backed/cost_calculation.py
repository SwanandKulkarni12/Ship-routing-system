import numpy as np
import logging
logger = logging.getLogger(__name__)

def safe_get(data, key, default):
    try:
        value = data[key]
        return float(value) if isinstance(value, (int, float)) else default
    except (KeyError, TypeError, ValueError):
        return default

def _angular_opposition(actual_dir, desired_bearing):
    diff = min(abs(actual_dir - desired_bearing), 360 - abs(actual_dir - desired_bearing))
    return (1.0 - np.cos(np.radians(diff))) / 2.0

def compute_safety_risk(current_weather, current_marine):
    wind = safe_get(current_weather, 'wind_speed_10m', 0.0)
    wave = safe_get(current_marine, 'wave_height', 0.0)
    precip = safe_get(current_weather, 'precipitation', 0.0)
    vis = safe_get(current_weather, 'visibility', 10000.0)
    
    # Dead zone thresholds — no penalty below these
    WAVE_THRESHOLD = 2.2   # Sea State 4 boundary
    WIND_THRESHOLD = 28.0  # ~15 knots, light breeze
    
    # Full severity references — aligned to WMO Sea State 7 / Beaufort 9
    # At these values severity = 1.0 (100%).  Values beyond are capped at 1.2.
    WAVE_FULL_SEV = 8.0    # Sea State 7 "High" — course change mandatory
    WIND_FULL_SEV = 90.0   # Beaufort 9 "Strong Gale"
    
    if wave <= WAVE_THRESHOLD:
        wave_sev = 0.0
    else:
        wave_sev = min(((wave - WAVE_THRESHOLD) / (WAVE_FULL_SEV - WAVE_THRESHOLD)) ** 2, 1.2)
        
    if wind <= WIND_THRESHOLD:
        wind_sev = 0.0
    else:
        wind_sev = min(((wind - WIND_THRESHOLD) / (WIND_FULL_SEV - WIND_THRESHOLD)) ** 2, 1.2)
        
    precip_sev = min(precip / 10.0, 1.0)
    vis_risk = max(0.0, 1.0 - min(vis / 10000.0, 1.0))
    
    return 0.45 * wave_sev + 0.35 * wind_sev + 0.15 * vis_risk + 0.05 * precip_sev

def calculate_weather_cost(weather_data, desired_bearing):
    try:
        cw = weather_data.get('weather', {}).get('current', {})
        cm = weather_data.get('marine', {}).get('current', {})
        wind_speed = safe_get(cw, 'wind_speed_10m', 0.0)
        wind_dir = safe_get(cw, 'wind_direction_10m', desired_bearing) % 360
        wave_height = safe_get(cm, 'wave_height', 0.0)
        wave_dir = safe_get(cm, 'wave_direction', desired_bearing) % 360
        curr_vel = safe_get(cm, 'ocean_current_velocity', 0.0)
        curr_dir = safe_get(cm, 'ocean_current_direction', desired_bearing) % 360
        wind_mag = 0.0 if wind_speed <= 28.0 else min(((wind_speed - 28.0) / 34.0) ** 2, 1.0)
        wave_mag = 0.0 if wave_height <= 2.2 else min(((wave_height - 2.2) / 2.3) ** 2, 1.0)
        curr_mag = min(curr_vel / 5.0, 1.0)
        wind_opp = _angular_opposition(wind_dir, desired_bearing)
        wave_opp = _angular_opposition(wave_dir, desired_bearing)
        curr_opp = _angular_opposition(curr_dir, desired_bearing)
        return 0.55 * wave_mag * wave_opp + 0.35 * wind_mag * wind_opp + 0.10 * curr_mag * curr_opp
    except Exception as e:
        logger.warning('Error calculating weather cost: %s', e)
        return 0.5