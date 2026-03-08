import requests_cache
from retry_requests import retry
import openmeteo_requests
import logging
logger = logging.getLogger(__name__)
class OpenMeteoRateLimitError(Exception):
    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after
cache_session = requests_cache.CachedSession('.weather_cache', expire_after=21600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.5)
openmeteo = openmeteo_requests.Client(session=retry_session)
def _is_rate_limit_error(exc):
    response = getattr(exc, 'response', None)
    if response is not None and getattr(response, 'status_code', None) == 429:
        return True
    message = str(exc).lower()
    return ('429' in message or 'too many requests' in message or 'rate limit' in message
            or 'limit exceeded' in message or 'try again in one minute' in message
            or 'minutely api request limit exceeded' in message)
def _extract_retry_after_seconds(exc):
    message = str(exc).lower()
    if 'one minute' in message:
        return 60
    return None
def fetch_weather_data_hourly(lat, lon, forecast_hours=72):
    url = 'https://api.open-meteo.com/v1/forecast'
    params = {
        'latitude': lat, 'longitude': lon,
        'hourly': ['wind_speed_10m', 'wind_direction_10m', 'precipitation', 'visibility'],
        'forecast_days': max(1, forecast_hours // 24 + 1),
        'wind_speed_unit': 'kmh',
    }
    try:
        responses = openmeteo.weather_api(url, params=params)
        result = []
        for response in responses:
            hourly = response.Hourly()
            speeds = hourly.Variables(0).ValuesAsNumpy()
            directions = hourly.Variables(1).ValuesAsNumpy()
            precip = hourly.Variables(2).ValuesAsNumpy()
            visibility = hourly.Variables(3).ValuesAsNumpy()
            n = min(len(speeds), forecast_hours)
            result.append([{
                'wind_speed_10m': float(speeds[i]),
                'wind_direction_10m': float(directions[i]),
                'precipitation': float(precip[i]),
                'visibility': float(visibility[i]),
            } for i in range(n)])
        return result
    except Exception as e:
        if _is_rate_limit_error(e):
            retry_after = _extract_retry_after_seconds(e)
            raise OpenMeteoRateLimitError('Open-Meteo hourly forecast API rate limited.', retry_after=retry_after) from e
        logger.warning('Hourly weather API error: %s', e)
        fallback = [{'wind_speed_10m': 10.0, 'wind_direction_10m': 180.0, 'precipitation': 0.0, 'visibility': 10000.0}] * forecast_hours
        return [fallback for _ in lat]
def fetch_marine_data_hourly(lat, lon, forecast_hours=72):
    url = 'https://marine-api.open-meteo.com/v1/marine'
    params = {
        'latitude': lat, 'longitude': lon,
        'hourly': ['wave_height', 'wave_direction', 'ocean_current_velocity', 'ocean_current_direction'],
        'forecast_days': max(1, forecast_hours // 24 + 1),
    }
    try:
        responses = openmeteo.weather_api(url, params=params)
        result = []
        for response in responses:
            hourly = response.Hourly()
            wave_h = hourly.Variables(0).ValuesAsNumpy()
            wave_d = hourly.Variables(1).ValuesAsNumpy()
            curr_v = hourly.Variables(2).ValuesAsNumpy()
            curr_d = hourly.Variables(3).ValuesAsNumpy()
            n = min(len(wave_h), forecast_hours)
            result.append([{
                'wave_height': float(wave_h[i]),
                'wave_direction': float(wave_d[i]),
                'ocean_current_velocity': float(curr_v[i]),
                'ocean_current_direction': float(curr_d[i]),
            } for i in range(n)])
        return result
    except Exception as e:
        if _is_rate_limit_error(e):
            retry_after = _extract_retry_after_seconds(e)
            raise OpenMeteoRateLimitError('Open-Meteo marine hourly API rate limited.', retry_after=retry_after) from e
        logger.warning('Hourly marine API error: %s', e)
        fallback = [{'wave_height': 1.0, 'wave_direction': 180.0, 'ocean_current_velocity': 2.0, 'ocean_current_direction': 180.0}] * forecast_hours
        return [fallback for _ in lat]