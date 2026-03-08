import os
import logging
import numpy as np
logger = logging.getLogger(__name__)

# Silencing chatty industrial libraries
for lib in ['copernicusmarine', 'urllib3', 'requests', 'aiohttp']:
    logging.getLogger(lib).setLevel(logging.ERROR)

def cmems_available() -> bool:
    try:
        import copernicusmarine
    except ImportError:
        return False
    if os.getenv('COPERNICUSMARINE_SERVICE_USERNAME') and os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD'):
        return True
    cred_file = os.path.expanduser('~/.copernicusmarine/copernicusmarine.json')
    return os.path.exists(cred_file)
_PHYSICS_DATASET = 'cmems_mod_glo_phy_anfc_0.083deg_PT1H-m'
_WAVE_DATASET = 'cmems_mod_glo_wav_anfc_0.083deg_PT3H-i'
_PHYSICS_VARS = ['uo', 'vo']
_WAVE_VARS = ['VHM0', 'VMDR']

def fetch_cmems_marine_grid(min_lat: float, max_lat: float, min_lon: float, max_lon: float, forecast_hours: int=72) -> dict | None:
    if not cmems_available():
        logger.debug('[cmems] not available — skipping')
        return None
    try:
        import copernicusmarine as cm
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        end_dt = now + timedelta(hours=int(forecast_hours))
        start_str = now.strftime('%Y-%m-%dT%H:%M:%S')
        end_str = end_dt.strftime('%Y-%m-%dT%H:%M:%S')
        pad = 0.5
        bbox = dict(minimum_latitude=float(min_lat) - pad, maximum_latitude=float(max_lat) + pad, minimum_longitude=float(min_lon) - pad, maximum_longitude=float(max_lon) + pad, start_datetime=start_str, end_datetime=end_str)
        logger.info('[cmems] fetching physics+wave grids  lat=[%.2f,%.2f] lon=[%.2f,%.2f] hours=%s', bbox['minimum_latitude'], bbox['maximum_latitude'], bbox['minimum_longitude'], bbox['maximum_longitude'], forecast_hours)
        username = os.getenv('COPERNICUSMARINE_SERVICE_USERNAME')
        password = os.getenv('COPERNICUSMARINE_SERVICE_PASSWORD')
        cred_kwargs = {}
        if username and password:
            cred_kwargs = {'username': username, 'password': password}
        phys_ds = cm.open_dataset(dataset_id=_PHYSICS_DATASET, variables=_PHYSICS_VARS, **bbox, **cred_kwargs).isel(depth=0).load()
        wave_ds = cm.open_dataset(dataset_id=_WAVE_DATASET, variables=_WAVE_VARS, **bbox, **cred_kwargs).load()
        lats = phys_ds['latitude'].values.tolist()
        lons = phys_ds['longitude'].values.tolist()
        uo = phys_ds['uo'].values
        vo = phys_ds['vo'].values
        wh = wave_ds['VHM0'].values
        wdir = wave_ds['VMDR'].values
        n_hours_phys = min(uo.shape[0], int(forecast_hours))
        n_hours_wave = min(wh.shape[0], int(forecast_hours))
        result = {'lats': lats, 'lons': lons, 'current_u': _safe_2d(uo[0]), 'current_v': _safe_2d(vo[0]), 'wave_height': _safe_2d(wh[0]), 'wave_dir': _safe_2d(wdir[0]), 'n_hours_physics': n_hours_phys, 'n_hours_wave': n_hours_wave}
        logger.info('[cmems] fetch complete  lats=%s lons=%s phys_hours=%s wave_hours=%s', len(lats), len(lons), n_hours_phys, n_hours_wave)
        return result
    except Exception as exc:
        logger.warning('[cmems] fetch failed: %s — falling back to Open-Meteo marine', exc)
        return None

def interpolate_cmems_at_point(cmems_grid: dict, lat: float, lon: float) -> dict:
    if cmems_grid is None:
        return {}
    lats = np.array(cmems_grid['lats'], dtype=np.float32)
    lons = np.array(cmems_grid['lons'], dtype=np.float32)
    dlat = lats - float(lat)
    dlon = lons - float(lon)
    lat_idx = int(np.argmin(np.abs(dlat)))
    lon_idx = int(np.argmin(np.abs(dlon)))
    lat_idxs = [max(0, lat_idx - 1), lat_idx, min(len(lats) - 1, lat_idx + 1)]
    lon_idxs = [max(0, lon_idx - 1), lon_idx, min(len(lons) - 1, lon_idx + 1)]
    wh_grid = np.array(cmems_grid['wave_height'], dtype=np.float32)
    wd_grid = np.array(cmems_grid['wave_dir'], dtype=np.float32)
    cu_grid = np.array(cmems_grid['current_u'], dtype=np.float32)
    cv_grid = np.array(cmems_grid['current_v'], dtype=np.float32)
    weights, wh_vals, wd_sin, wd_cos, cu_vals, cv_vals = ([], [], [], [], [], [])
    for li in lat_idxs:
        for lj in lon_idxs:
            d = max(np.hypot(lats[li] - lat, lons[lj] - lon), 1e-06)
            w = 1.0 / d ** 2
            weights.append(w)
            wh_vals.append(_safe_val(wh_grid, li, lj))
            wd_rad = np.radians(_safe_val(wd_grid, li, lj, default=180.0))
            wd_sin.append(np.sin(wd_rad))
            wd_cos.append(np.cos(wd_rad))
            cu_vals.append(_safe_val(cu_grid, li, lj))
            cv_vals.append(_safe_val(cv_grid, li, lj))
    wt = np.array(weights)
    wt /= wt.sum()
    wh = float(np.dot(wt, wh_vals))
    wds = float(np.dot(wt, wd_sin))
    wdc = float(np.dot(wt, wd_cos))
    cu = float(np.dot(wt, cu_vals))
    cv = float(np.dot(wt, cv_vals))
    wave_dir = (np.degrees(np.arctan2(wds, wdc)) + 360) % 360
    current_speed_ms = np.hypot(cu, cv)
    current_dir = (np.degrees(np.arctan2(cu, cv)) + 360) % 360
    return {'wave_height': round(max(wh, 0.0), 3), 'wave_direction': round(wave_dir, 1), 'ocean_current_velocity': round(current_speed_ms * 3.6, 3), 'ocean_current_direction': round(current_dir, 1)}

def _safe_2d(arr) -> list:
    a = np.where(np.isfinite(arr), arr, 0.0)
    return a.tolist()

def _safe_val(grid, li, lj, default=0.0) -> float:
    try:
        v = float(grid[li, lj])
        return v if np.isfinite(v) else default
    except (IndexError, TypeError, ValueError):
        return default