import logging
import os
import tempfile
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import numpy as np
import requests
logger = logging.getLogger(__name__)
_NOMADS_BASE = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl'
_MAX_STEP_H = 72
_STEP_HOURS = list(range(0, _MAX_STEP_H + 1, 3))
_PARALLEL_DL = 8
_DL_TIMEOUT_S = 45
_PUBLISH_DELAY_H = 4
_MAX_RUN_TRIES = 3
def gfs_available() -> bool:
    try:
        import cfgrib
        return True
    except ImportError:
        return False
def _latest_gfs_run(now_utc: datetime, offset_runs: int=0) -> tuple[str, str]:
    lag = now_utc - timedelta(hours=_PUBLISH_DELAY_H + offset_runs * 6)
    run_hour = lag.hour // 6 * 6
    run_time = lag.replace(hour=run_hour, minute=0, second=0, microsecond=0)
    return (run_time.strftime('%Y%m%d'), f'{run_hour:02d}')
def _nomads_url(date_str: str, run_str: str, step: int, lat_s: float, lat_n: float, lon_w: float, lon_e: float) -> str:
    fname = f'gfs.t{run_str}z.pgrb2.0p25.f{step:03d}'
    dir_path = f'%2Fgfs.{date_str}%2F{run_str}%2Fatmos'
    return f'{_NOMADS_BASE}?file={fname}&lev_10_m_above_ground=on&lev_surface=on&var_UGRD=on&var_VGRD=on&var_APCP=on&subregion=&leftlon={lon_w:.2f}&rightlon={lon_e:.2f}&toplat={lat_n:.2f}&bottomlat={lat_s:.2f}&dir={dir_path}'
_GRIB_MAGIC = b'GRIB'
def _download_step(step: int, url: str, tmp_dir: str) -> tuple[int, str] | None:
    out_path = os.path.join(tmp_dir, f'gfs_f{step:03d}.grib2')
    try:
        r = requests.get(url, timeout=_DL_TIMEOUT_S, stream=True)
        r.raise_for_status()
        with open(out_path, 'wb') as fh:
            for chunk in r.iter_content(chunk_size=65536):
                fh.write(chunk)
        with open(out_path, 'rb') as fh:
            magic = fh.read(4)
        if magic != _GRIB_MAGIC:
            logger.debug('[gfs] step %3dh: not a GRIB file (got %r) — run not published yet', step, magic)
            return None
        return (step, out_path)
    except Exception as exc:
        logger.debug('[gfs] step %3dh download error: %s', step, exc)
        return None
def _parse_step_grib(path: str) -> tuple | None:
    import cfgrib
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', FutureWarning)
            datasets = cfgrib.open_datasets(path, backend_kwargs={'indexpath': ''})
    except Exception as exc:
        logger.debug('[gfs] open_datasets failed: %s', exc)
        return None
    if not datasets:
        logger.debug('[gfs] open_datasets returned 0 datasets for %s', path)
        return None
    logger.debug('[gfs] GRIB datasets: %s', [{v: ds[v].attrs.get('GRIB_shortName', '?') + '/' + ds[v].attrs.get('GRIB_typeOfLevel', '?') + '/' + str(ds[v].attrs.get('GRIB_level', '?')) for v in ds.data_vars} for ds in datasets])
    u10 = v10 = tp = lats = lons = None
    for ds in datasets:
        if 'latitude' not in ds.coords or 'longitude' not in ds.coords:
            ds.close()
            continue
        raw_lats = ds.coords['latitude'].values
        raw_lons = ds.coords['longitude'].values
        lat_ord = np.argsort(raw_lats)
        for var in ds.data_vars:
            arr = ds[var].values
            if arr.ndim != 2:
                continue
            attrs = ds[var].attrs
            tol = attrs.get('GRIB_typeOfLevel', '')
            short_name = attrs.get('GRIB_shortName', var).lower()
            step_type = attrs.get('GRIB_stepType', '')
            arr_s = arr[lat_ord].astype(np.float32)
            is_wind_level = tol == 'heightAboveGround'
            is_u = any((x in short_name for x in ('10u', 'ugrd', 'u10'))) or (is_wind_level and 'u' in short_name and ('v' not in short_name))
            is_v = any((x in short_name for x in ('10v', 'vgrd', 'v10'))) or (is_wind_level and 'v' in short_name and ('u' not in short_name))
            is_precip = tol == 'surface' and (step_type == 'accum' or any((x in short_name for x in ('tp', 'acpcp', 'apcp', 'prate'))))
            if is_wind_level and is_u and (u10 is None):
                u10 = arr_s
                lats = raw_lats[lat_ord]
                lons = raw_lons
            elif is_wind_level and is_v and (v10 is None):
                v10 = arr_s
                if lats is None:
                    lats = raw_lats[lat_ord]
                    lons = raw_lons
            elif is_wind_level and u10 is None:
                u10 = arr_s
                lats = raw_lats[lat_ord]
                lons = raw_lons
            elif is_wind_level and v10 is None:
                v10 = arr_s
            elif is_precip and tp is None:
                tp = arr_s
                if lats is None:
                    lats = raw_lats[lat_ord]
                    lons = raw_lons
        ds.close()
    if u10 is None:
        logger.debug('[gfs] no 10m wind found in GRIB — datasets had vars: %s', [[list(d.data_vars) for d in datasets]])
        return None
    if v10 is None:
        v10 = np.zeros_like(u10)
    if tp is None:
        tp = np.zeros_like(u10)
    return (u10, v10, tp, lats, lons)
def fetch_gfs_atmospheric_grid(min_lat: float, max_lat: float, min_lon: float, max_lon: float, forecast_hours: int=72) -> dict | None:
    if not gfs_available():
        logger.debug('[gfs] cfgrib/xarray not available — skipping')
        return None
    steps = [s for s in _STEP_HOURS if s <= int(forecast_hours)]
    if not steps:
        steps = [0]
    pad = 0.5
    lat_s = max(-90.0, float(min_lat) - pad)
    lat_n = min(90.0, float(max_lat) + pad)
    lon_w = max(-180.0, float(min_lon) - pad)
    lon_e = min(180.0, float(max_lon) + pad)
    now_utc = datetime.now(timezone.utc)
    for offset in range(_MAX_RUN_TRIES):
        date_str, run_str = _latest_gfs_run(now_utc, offset_runs=offset)
        label = f'run={date_str}/{run_str}Z' + (f' (fallback -{offset * 6}h)' if offset else '')
        logger.info('[gfs] %s  bbox=N%.1f/W%.1f/S%.1f/E%.1f  steps=0..%dh  downloads=%d', label, lat_n, lon_w, lat_s, lon_e, steps[-1], len(steps))
        try:
            with tempfile.TemporaryDirectory() as tmp:
                step_files: dict[int, str] = {}
                with ThreadPoolExecutor(max_workers=_PARALLEL_DL) as pool:
                    futs = {pool.submit(_download_step, step, _nomads_url(date_str, run_str, step, lat_s, lat_n, lon_w, lon_e), tmp): step for step in steps}
                    for fut in as_completed(futs):
                        res = fut.result()
                        if res is not None:
                            step_files[res[0]] = res[1]
                if not step_files:
                    logger.warning('[gfs] %s — 0 steps downloaded, trying previous run', label)
                    continue
                available_steps = sorted(step_files.keys())
                logger.info('[gfs] downloaded %d/%d steps', len(available_steps), len(steps))
                parsed: dict[int, tuple] = {}
                for step in available_steps:
                    result = _parse_step_grib(step_files[step])
                    if result is not None:
                        parsed[step] = result
                if not parsed:
                    logger.warning('[gfs] %s — GRIB parse failed for all steps', label)
                    continue
                good_steps = sorted(parsed.keys())
                _, _, _, lats, lons = parsed[good_steps[0]]
                n_lat, n_lon, n_steps = (len(lats), len(lons), len(good_steps))
                u10_arr = np.zeros((n_steps, n_lat, n_lon), dtype=np.float32)
                v10_arr = np.zeros((n_steps, n_lat, n_lon), dtype=np.float32)
                tp_arr = np.zeros((n_steps, n_lat, n_lon), dtype=np.float32)
                prev_tp = np.zeros((n_lat, n_lon), dtype=np.float32)
                for i, step in enumerate(good_steps):
                    u, v, tp_acc, _, _ = parsed[step]
                    u10_arr[i] = u
                    v10_arr[i] = v
                    if i == 0:
                        tp_arr[i] = np.maximum(tp_acc, 0.0)
                    else:
                        dt_h = max(good_steps[i] - good_steps[i - 1], 1)
                        tp_arr[i] = np.maximum((tp_acc - prev_tp) / dt_h, 0.0)
                    prev_tp = tp_acc
                logger.info('[gfs] grid ready  lats=%d lons=%d steps=%d  %s', n_lat, n_lon, n_steps, label)
                return {'lats': lats.tolist(), 'lons': lons.tolist(), 'steps': good_steps, 'wind_u10': u10_arr, 'wind_v10': v10_arr, 'precipitation': tp_arr}
        except Exception as exc:
            logger.warning('[gfs] %s failed: %s — trying previous run', label, exc)
            continue
    logger.warning('[gfs] all %d run attempts failed — falling back to Open-Meteo', _MAX_RUN_TRIES)
    return None
def interpolate_gfs_at_point(grid: dict, lat: float, lon: float, hour: int=0) -> dict:
    if grid is None:
        return {}
    try:
        lats = np.array(grid['lats'], dtype=np.float32)
        lons = np.array(grid['lons'], dtype=np.float32)
        steps = grid['steps']
        step_idx = int(np.argmin(np.abs(np.array(steps) - int(hour))))
        u10 = grid['wind_u10'][step_idx]
        v10 = grid['wind_v10'][step_idx]
        tp = grid['precipitation'][step_idx]
        lat_idx = int(np.argmin(np.abs(lats - float(lat))))
        lon_idx = int(np.argmin(np.abs(lons - float(lon))))
        li_range = [max(0, lat_idx - 1), lat_idx, min(len(lats) - 1, lat_idx + 1)]
        lj_range = [max(0, lon_idx - 1), lon_idx, min(len(lons) - 1, lon_idx + 1)]
        weights, u_vals, v_vals, tp_vals = ([], [], [], [])
        for li in li_range:
            for lj in lj_range:
                d = max(np.hypot(lats[li] - lat, lons[lj] - lon), 1e-06)
                w = 1.0 / d ** 2
                weights.append(w)
                u_vals.append(float(u10[li, lj]))
                v_vals.append(float(v10[li, lj]))
                tp_vals.append(float(tp[li, lj]))
        wt = np.array(weights)
        wt /= wt.sum()
        u = float(np.dot(wt, u_vals))
        v = float(np.dot(wt, v_vals))
        p = float(np.dot(wt, tp_vals))
        speed_kmh = float(np.hypot(u, v) * 3.6)
        wind_dir = float((np.degrees(np.arctan2(-u, -v)) + 360) % 360)
        return {'wind_speed_10m': round(speed_kmh, 2), 'wind_direction_10m': round(wind_dir, 1), 'precipitation': round(max(p, 0.0), 3)}
    except Exception as exc:
        logger.debug('[gfs] interpolation failed: %s', exc)
        return {}
def build_hourly_weather_lookup_from_gfs(grid_points: list, gfs_grid: dict, forecast_hours: int=72) -> dict:
    lookup = {}
    steps = gfs_grid.get('steps', [0])
    for loc in grid_points:
        lat_pt, lon_pt = (float(loc[0]), float(loc[1]))
        hourly = []
        for h in range(int(forecast_hours)):
            nearest_step = min(steps, key=lambda s: abs(s - h))
            pt = interpolate_gfs_at_point(gfs_grid, lat_pt, lon_pt, hour=nearest_step)
            hourly.append({'wind_speed_10m': pt.get('wind_speed_10m', 10.0), 'wind_direction_10m': pt.get('wind_direction_10m', 180.0), 'precipitation': pt.get('precipitation', 0.0), 'visibility': 10000.0})
        lookup[loc] = hourly
    return lookup