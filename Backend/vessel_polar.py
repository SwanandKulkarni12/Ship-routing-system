from math import radians, sin, cos, sqrt, atan2, degrees
BASE_SPEED_KNOTS = 14.0
BASE_FUEL_TONNES_PER_DAY = 35.0
CO2_FACTOR_HFO = 3.206
R_NM = 3440.065
POLAR_SPEED_FACTOR = {
    (0, 0): 1.0, (0, 1): 0.99, (0, 2): 0.98, (0, 3): 0.97, (0, 4): 0.96, (0, 5): 0.97, (0, 6): 0.98, (0, 7): 0.99, 
    (1, 0): 0.98, (1, 1): 0.96, (1, 2): 0.94, (1, 3): 0.92, (1, 4): 0.90, (1, 5): 0.92, (1, 6): 0.94, (1, 7): 0.96, 
    (2, 0): 0.95, (2, 1): 0.92, (2, 2): 0.88, (2, 3): 0.84, (2, 4): 0.80, (2, 5): 0.84, (2, 6): 0.88, (2, 7): 0.92, 
    (3, 0): 0.90, (3, 1): 0.85, (3, 2): 0.80, (3, 3): 0.75, (3, 4): 0.70, (3, 5): 0.75, (3, 6): 0.80, (3, 7): 0.85, 
    (4, 0): 0.85, (4, 1): 0.78, (4, 2): 0.70, (4, 3): 0.65, (4, 4): 0.60, (4, 5): 0.65, (4, 6): 0.70, (4, 7): 0.78, 
    (5, 0): 0.75, (5, 1): 0.68, (5, 2): 0.60, (5, 3): 0.55, (5, 4): 0.50, (5, 5): 0.55, (5, 6): 0.60, (5, 7): 0.68
}
MAX_FUEL_SURGE_FACTOR = 2.5
MIN_FUEL_EFFICIENCY_FACTOR = 0.6

def get_speed_factor(wave_height_m: float) -> float:
    import math
    wh = float(wave_height_m) if wave_height_m is not None else 0.0
    if not math.isfinite(wh):
        wh = 0.0
    h_bin = min(int(max(wh, 0.0) // 1.0), 5)
    factors = [POLAR_SPEED_FACTOR.get((h_bin, d), 0.3) for d in range(8)]
    return float(sum(factors) / len(factors))

def get_speed_factor_polar(wave_height_m: float, wave_direction_deg: float, ship_heading_deg: float) -> float:
    import math
    wh = float(wave_height_m) if wave_height_m is not None else 0.0
    wd = float(wave_direction_deg) if wave_direction_deg is not None else 0.0
    if not math.isfinite(wh):
        wh = 0.0
    if not math.isfinite(wd):
        wd = 0.0
    h_bin = min(int(max(wh, 0.0) // 1.0), 5)
    rel_dir = (wd - float(ship_heading_deg)) % 360.0
    d_bin = int(rel_dir // 45.0) % 8
    return float(POLAR_SPEED_FACTOR.get((h_bin, d_bin), 0.25))

def _segment_nm(lat1, lon1, lat2, lon2) -> float:
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * R_NM * atan2(sqrt(a), sqrt(1 - a))

def _bearing_deg(lat1, lon1, lat2, lon2) -> float:
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    x = sin(dlon) * cos(lat2)
    y = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)
    return (degrees(atan2(x, y)) + 360.0) % 360.0

def compute_vessel_performance(dist_nm: float, wave_h: float, wave_d: float, wind_s_kmh: float, wind_d: float, curr_v_kmh: float, curr_d: float, heading: float) -> dict:
    import math as _math
    # 1. Base Wave Speed Factor from Polar
    sf_base = get_speed_factor_polar(wave_h, wave_d, heading)
    
    # 2. Wind/Current Impact (Simplified but Physical)
    # Wind penalty (~0.1 loss per 50kmh headwind)
    rel_wind = (wind_d - heading) % 360.0
    wind_penalty = 0.2 * (wind_s_kmh / 90.0) * _math.cos(_math.radians(rel_wind))
    
    # Current boost/loss
    rel_curr = (curr_d - heading) % 360.0
    curr_boost_kn = (curr_v_kmh / 1.852) * _math.cos(_math.radians(rel_curr))
    
    # 3. Effective Speed Factor (Capped at 0.05 for physics, but surge cap will apply later)
    # Note: penalty is additive here
    effective_sf = max(sf_base - wind_penalty, 0.05)
    
    # 4. Resulting Speed
    water_speed = max(BASE_SPEED_KNOTS * effective_sf, 0.1)
    ground_speed = max(water_speed + curr_boost_kn, 0.1)
    
    # 5. Time and Fuel
    time_h = dist_nm / ground_speed
    
    # Economics: Surge Cap (Crucial for avoiding 20 Billion Dollar costs)
    # We cap the EFFORT at 2.5x base. This applies to both fuel and virtual charter cost.
    effort_multiplier = 1.0 / effective_sf
    bounded_surge = max(MIN_FUEL_EFFICIENCY_FACTOR, min(effort_multiplier, MAX_FUEL_SURGE_FACTOR))
    
    # Base daily burn is 35t @ 14kt.
    base_tonnes_per_nm = BASE_FUEL_TONNES_PER_DAY / (BASE_SPEED_KNOTS * 24.0)
    fuel_t = dist_nm * base_tonnes_per_nm * bounded_surge
    
    return {
        'time_hours': round(time_h, 3),
        'fuel_tonnes': round(fuel_t, 3),
        'speed_kn': round(ground_speed, 2),
        'effective_sf': round(effective_sf, 3),
        'effort_multiplier': round(bounded_surge, 3)
    }

def calculate_eta_hours(path_latlon: list, weather_info_list: list) -> float:
    return calculate_fuel_and_co2(path_latlon, weather_info_list)['eta_hours']

def calculate_fuel_and_co2(path_latlon: list, weather_info_list: list) -> dict:
    total_fuel = 0.0
    total_time = 0.0
    for i in range(len(path_latlon) - 1):
        lat1, lon1 = path_latlon[i]
        lat2, lon2 = path_latlon[i + 1]
        dist_nm = _segment_nm(lat1, lon1, lat2, lon2)
        w = weather_info_list[i] if i < len(weather_info_list) else {}
        perf = compute_vessel_performance(
            dist_nm,
            float(w.get('wave_height', 0.0)),
            float(w.get('wave_dir', 0.0)),
            float(w.get('wind_speed', 0.0)),
            float(w.get('wind_dir', 0.0)),
            float(w.get('ocean_current_velocity', 0.0)),
            float(w.get('ocean_current_direction', 0.0)),
            _bearing_deg(lat1, lon1, lat2, lon2)
        )
        total_fuel += perf['fuel_tonnes']
        total_time += perf['time_hours']
    
    fuel_t = round(total_fuel, 2)
    return {
        'fuel_tonnes': fuel_t,
        'co2_tonnes': round(fuel_t * CO2_FACTOR_HFO, 2),
        'eta_hours': round(total_time, 2)
    }