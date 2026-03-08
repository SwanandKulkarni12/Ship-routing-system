import { useEffect, useRef, useState, useCallback } from 'react';
import './App.css';
import { EMPTY_GEOJSON, TRANSPARENT_1PX } from './constants';
import {
  buildWeatherRasterCanvas,
  computeWeatherRiskFromPoint,
  buildRouteRiskPoints,
  interpolateSeriesValue,
  riskToColor,
  clampValue,
} from './utils/riskUtils';
import { isWaterCoordinate, interpolateRoutePos, routeBearingAt } from './utils/geoUtils';
import MetricsPanel  from './components/MetricsPanel';
import ResultBanner  from './components/ResultBanner';
import {
  headerStyle, panelStyle, inputStyle, selectOptionStyle,
  buttonStyle, spinnerStyle, distanceCardStyle,
  pitchCalloutStyle, pitchCalloutTitleStyle, pitchCalloutValuesStyle,
  toggleMetricsStyle, legendRowStyle, legendDotStyle,
  legendDockStyle, legendBarStyle, legendUnitStyle,
  legendGradientStyle, legendTicksStyle,
} from './styles';
const MLMap             = window.maplibregl?.Map;
const NavigationControl = window.maplibregl?.NavigationControl;
function App() {
  const [astarcoordinates, setAstarCoordinates] = useState([]);
  const [coordinates,      setCoordinates]      = useState([]);
  const [severityData,     setSeverityData]      = useState([]);
  const [astarWeather,     setAstarWeather]      = useState([]);
  const [optimizedWeather, setOptimizedWeather]  = useState([]);
  const [metrics,          setMetrics]           = useState(null);
  const [reportUrl,        setReportUrl]         = useState(null);
  const [excelUrl,         setExcelUrl]          = useState(null);
  const [progress,         setProgress]          = useState({ pct: 0, step: '' });
  const [mode,             setMode]              = useState('balanced');
  const [start,            setStart]             = useState('');
  const [end,              setEnd]               = useState('');
  const [status,           setStatus]            = useState('Connected');
  const [distance,         setDistance]          = useState(0);
  const [showMetrics,      setShowMetrics]        = useState(false);
  const [coordWarning,     setCoordWarning]      = useState('');
  const [shipAnimating,    setShipAnimating]     = useState(false);
  const [pickMode,         setPickMode]          = useState(null);
  const [inputMode,        setInputMode]         = useState('map');
  const [showRouteDots,    setShowRouteDots]     = useState(false);
  const [manualStart,      setManualStart]       = useState('');
  const [manualEnd,        setManualEnd]         = useState('');
  const mapContainerRef = useRef(null);
  const mapRef          = useRef(null);
  const wsRef           = useRef(null);
  const pickModeRef     = useRef(null);
  const startMarkerRef  = useRef(null);
  const endMarkerRef    = useRef(null);
  const coordsRef        = useRef([]);
  const astarCoordsRef   = useRef([]);
  const shipAnimatingRef = useRef(false);
  useEffect(() => { pickModeRef.current     = pickMode;      }, [pickMode]);
  useEffect(() => { shipAnimatingRef.current = shipAnimating; }, [shipAnimating]);
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    map.getCanvas().style.cursor = pickMode ? 'crosshair' : '';
  }, [pickMode]);
  const clearRouteState = useCallback(() => {
    setCoordinates([]);
    setAstarCoordinates([]);
    setSeverityData([]);
    setAstarWeather([]);
    setOptimizedWeather([]);
    setMetrics(null);
    setDistance(0);
    setShowMetrics(false);
    setCoordWarning('');
    setStatus('Ready');
    setShipAnimating(false);
    coordsRef.current      = [];
    astarCoordsRef.current = [];
    const map = mapRef.current;
    if (map) {
      if (map.getSource('ship-opt'))     map.getSource('ship-opt').setData(EMPTY_GEOJSON);
      if (map.getSource('ship-astar'))   map.getSource('ship-astar').setData(EMPTY_GEOJSON);
      if (map.getSource('danger-zones')) map.getSource('danger-zones').setData(EMPTY_GEOJSON);
      if (map.getSource('weather-raster')) {
        map.getSource('weather-raster').updateImage({
          url: TRANSPARENT_1PX,
          coordinates: [[-180, 85], [180, 85], [180, -85], [-180, -85]],
        });
      }
    }
  }, []);
  const placeMarkerFromCoord = useCallback((coordStr, which) => {
    const map = mapRef.current;
    if (!map) return;
    const [lat, lon] = coordStr.split(',').map(Number);
    if (!isFinite(lat) || !isFinite(lon)) return;
    if (which === 'start') {
      if (startMarkerRef.current) startMarkerRef.current.remove();
      startMarkerRef.current = new window.maplibregl.Marker({ color: '#00e676' })
        .setLngLat([lon, lat]).addTo(map);
      setStart(coordStr);
    } else {
      if (endMarkerRef.current) endMarkerRef.current.remove();
      endMarkerRef.current = new window.maplibregl.Marker({ color: '#ff4444' })
        .setLngLat([lon, lat]).addTo(map);
      setEnd(coordStr);
    }
  }, []);
  const applyManualCoords = useCallback(() => {
    const sOk = manualStart.match(/^-?\d+\.?\d*,-?\d+\.?\d*$/);
    const eOk = manualEnd.match(/^-?\d+\.?\d*,-?\d+\.?\d*$/);
    if (!sOk || !eOk) {
      setCoordWarning('⚠ Enter coordinates as lat,lon — e.g. 40.6,-74.0');
      return;
    }
    clearRouteState();
    placeMarkerFromCoord(manualStart, 'start');
    placeMarkerFromCoord(manualEnd,   'end');
    setCoordWarning('');
  }, [manualStart, manualEnd, clearRouteState, placeMarkerFromCoord]);
  useEffect(() => {
    if (!mapContainerRef.current) return;
    const map = new MLMap({
      container: mapContainerRef.current,
      style: {
        version: 8,
        glyphs: 'https://fonts.openmaptiles.org/{fontstack}/{range}.pbf',
        sources: {
          world: {
            type: 'vector',
            url: 'https://demotiles.maplibre.org/tiles/tiles.json',
          },
          carto_labels: {
            type: 'raster',
            tiles: [
              'https://a.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}.png',
              'https://b.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}.png',
              'https://c.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}.png',
            ],
            tileSize: 256,
            attribution: '© OpenStreetMap © CARTO',
          },
        },
        layers: [
          { id: 'bg', type: 'background', paint: { 'background-color': '#0b1e36' } },
          { id: 'land', type: 'fill', source: 'world', 'source-layer': 'countries',
            paint: { 'fill-color': '#1c2333' } },
          { id: 'borders', type: 'line', source: 'world', 'source-layer': 'countries',
            paint: { 'line-color': 'rgba(255,255,255,0.18)', 'line-width': 0.7 } },
          { id: 'labels', type: 'raster', source: 'carto_labels',
            paint: { 'raster-opacity': 0.9 } },
        ],
      },
      center: [0, 20],
      zoom: 2,
    });
    map.addControl(new NavigationControl(), 'top-right');
    mapRef.current = map;
    map.on('click', (e) => {
      const mode = pickModeRef.current;
      if (!mode) return;
      const { lng, lat } = e.lngLat;
      const coordStr = `${lat.toFixed(5)},${lng.toFixed(5)}`;
      if (mode === 'start') {
        setStart(coordStr);
        if (startMarkerRef.current) startMarkerRef.current.remove();
        startMarkerRef.current = new window.maplibregl.Marker({ color: '#00e676' })
          .setLngLat([lng, lat]).addTo(map);
        setPickMode('end');
      } else {
        setEnd(coordStr);
        if (endMarkerRef.current) endMarkerRef.current.remove();
        endMarkerRef.current = new window.maplibregl.Marker({ color: '#ff4444' })
          .setLngLat([lng, lat]).addTo(map);
        setPickMode(null);
      }
      setCoordWarning('');
      const label = mode === 'start' ? 'Start' : 'End';
      isWaterCoordinate(lat, lng).then((isWater) => {
        if (!isWater) setCoordWarning(`⚠ ${label} point appears to be on land. Please select a water coordinate.`);
      });
    });
    map.on('load', () => {
      map.addSource('weather-raster', {
        type: 'image',
        url: TRANSPARENT_1PX,
        coordinates: [[-180, 85], [180, 85], [180, -85], [-180, -85]],
      });
      map.addLayer({
        id: 'weather-raster-layer', type: 'raster', source: 'weather-raster',
        paint: { 'raster-opacity': 0.88, 'raster-fade-duration': 300, 'raster-resampling': 'linear' },
      });
      map.addSource('severity-circles', { type: 'geojson', data: EMPTY_GEOJSON });
      map.addLayer({
        id: 'severity-circle-layer', type: 'circle', source: 'severity-circles', minzoom: 10,
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['get', 'risk'], 0, 4, 50, 7, 100, 10],
          'circle-color':  ['interpolate', ['linear'], ['get', 'risk'],
            0, '#1e6ec8', 30, '#46d278', 60, '#ffd232', 80, '#ff8200', 100, '#dc1414'],
          'circle-opacity': 0.80, 'circle-blur': 0.3,
        },
      });
      map.addSource('risk-segments', { type: 'geojson', data: EMPTY_GEOJSON });
      map.addLayer({ id: 'risk-astar-segments', type: 'line', source: 'risk-segments',
        filter: ['==', ['get', 'routeType'], 'astar'],
        paint: { 'line-width': 4, 'line-color': '#ffd060', 'line-opacity': 0.90 } });
      map.addLayer({ id: 'risk-opt-segments', type: 'line', source: 'risk-segments',
        filter: ['==', ['get', 'routeType'], 'optimized'],
        paint: { 'line-width': 4, 'line-color': '#00e676', 'line-opacity': 0.97 } });
      map.addSource('routes', { type: 'geojson', data: EMPTY_GEOJSON });
      map.addLayer({ id: 'astar-glow', type: 'line', source: 'routes',
        filter: ['==', ['get', 'routeType'], 'astar'],
        paint: { 'line-color': '#ffd060', 'line-width': 8, 'line-opacity': 0.18, 'line-blur': 4 } });
      map.addLayer({ id: 'optimized-glow', type: 'line', source: 'routes',
        filter: ['==', ['get', 'routeType'], 'optimized'],
        paint: { 'line-color': '#00e676', 'line-width': 16, 'line-opacity': 0.15, 'line-blur': 10 } });
      map.addLayer({ id: 'astar-line', type: 'line', source: 'routes',
        filter: ['==', ['get', 'routeType'], 'astar'],
        paint: { 'line-color': '#ffd060', 'line-width': 1.5, 'line-opacity': 0.30 } });
      const makeShipImage = (fillColor, glowColor, size = 48) => {
        const canvas = document.createElement('canvas');
        canvas.width = size; canvas.height = size;
        const ctx = canvas.getContext('2d');
        const cx = size / 2, cy = size / 2;
        const grad = ctx.createRadialGradient(cx, cy, 2, cx, cy, size * 0.44);
        grad.addColorStop(0, glowColor + 'aa');
        grad.addColorStop(1, glowColor + '00');
        ctx.fillStyle = grad;
        ctx.fillRect(0, 0, size, size);
        ctx.beginPath();
        ctx.moveTo(cx, cy - size * 0.40);          
        ctx.lineTo(cx + size * 0.22, cy - size * 0.05);
        ctx.lineTo(cx + size * 0.18, cy + size * 0.36);
        ctx.lineTo(cx - size * 0.18, cy + size * 0.36);
        ctx.lineTo(cx - size * 0.22, cy - size * 0.05);
        ctx.closePath();
        ctx.fillStyle = fillColor;
        ctx.fill();
        ctx.strokeStyle = '#ffffff';
        ctx.lineWidth = 1.8;
        ctx.stroke();
        ctx.fillStyle = '#ffffff44';
        ctx.fillRect(cx - size * 0.08, cy - size * 0.02, size * 0.16, size * 0.18);
        return ctx.getImageData(0, 0, size, size);
      };
      if (!map.hasImage('ship-icon-opt'))   map.addImage('ship-icon-opt',   makeShipImage('#00ffaa', '#00ffaa'));
      if (!map.hasImage('ship-icon-astar')) map.addImage('ship-icon-astar', makeShipImage('#ffd060', '#ffcc00'));
      map.addSource('ship-opt', { type: 'geojson', data: EMPTY_GEOJSON });
      map.addLayer({ id: 'ship-opt-icon', type: 'symbol', source: 'ship-opt',
        layout: {
          'icon-image': 'ship-icon-opt',
          'icon-size': 0.72,
          'icon-rotate': ['get', 'bearing'],
          'icon-rotation-alignment': 'map',
          'icon-allow-overlap': true,
          'icon-ignore-placement': true,
        },
      });
      map.addSource('ship-astar', { type: 'geojson', data: EMPTY_GEOJSON });
      map.addLayer({ id: 'ship-astar-icon', type: 'symbol', source: 'ship-astar',
        layout: {
          'icon-image': 'ship-icon-astar',
          'icon-size': 0.72,
          'icon-rotate': ['get', 'bearing'],
          'icon-rotation-alignment': 'map',
          'icon-allow-overlap': true,
          'icon-ignore-placement': true,
        },
      });
      map.addSource('route-waypoints', { type: 'geojson', data: EMPTY_GEOJSON });
      map.addLayer({ id: 'route-waypoints-hit', type: 'circle', source: 'route-waypoints',
        paint: { 'circle-radius': 8, 'circle-color': 'transparent', 'circle-opacity': 0 } });
      map.addLayer({ id: 'route-waypoints-dot', type: 'circle', source: 'route-waypoints',
        paint: {
          'circle-radius':       ['case', ['boolean', ['feature-state', 'hover'], false], 6, 3.5],
          'circle-color':        ['case', ['boolean', ['feature-state', 'hover'], false], '#ffffff', '#cccccc'],
          'circle-stroke-color': ['get', 'routeColor'],
          'circle-stroke-width': 1.5,
          'circle-opacity':      ['case', ['boolean', ['feature-state', 'hover'], false], 1, 0.6],
        } });
      const waypointPopup = new window.maplibregl.Popup({
        closeButton: false, closeOnClick: false, className: 'waypoint-popup', maxWidth: '280px',
      });
      let hoveredWaypointId = null;
      map.on('mouseenter', 'route-waypoints-hit', (e) => {
        map.getCanvas().style.cursor = 'crosshair';
        const f = e.features[0];
        if (!f) return;
        const p   = f.properties;
        const fmt = (v, dec = 1, unit = '') =>
          (v != null && v !== '' && !isNaN(Number(v))) ? `${Number(v).toFixed(dec)}${unit}` : '—';
        const riskVal   = p.risk != null ? Math.round(Number(p.risk)) : null;
        const riskColor = riskVal == null ? '#aaa'
          : riskVal < 33 ? '#4ade80' : riskVal < 60 ? '#facc15'
          : riskVal < 80 ? '#fb923c' : '#f87171';
        waypointPopup.setLngLat(e.lngLat).setHTML(`
          <div style="font-family:monospace;font-size:11px;line-height:1.6;color:#e2e8f0;">
            <div style="font-size:12px;font-weight:bold;color:#fff;border-bottom:1px solid #334155;padding-bottom:4px;margin-bottom:6px;">
              📍 ${fmt(p.lat,4)}°, ${fmt(p.lon,4)}°
              <span style="float:right;background:${riskColor};color:#000;border-radius:4px;padding:1px 6px;font-size:10px;">
                Risk ${riskVal != null ? riskVal + '%' : '—'}
              </span>
            </div>
            <table style="border-collapse:collapse;width:100%;">
              <tr><td style="color:#94a3b8;">🌊 Wave</td><td style="text-align:right;">${fmt(p.wave_height,1,' m')} @ ${fmt(p.wave_dir,0,'°')}</td></tr>
              <tr><td style="color:#94a3b8;">💨 Wind</td><td style="text-align:right;">${fmt(p.wind_speed,1,' km/h')} @ ${fmt(p.wind_direction,0,'°')}</td></tr>
              <tr><td style="color:#94a3b8;">🌧 Rain</td><td style="text-align:right;">${fmt(p.precipitation,1,' mm/h')}</td></tr>
              <tr><td style="color:#94a3b8;">👁 Visibility</td><td style="text-align:right;">${fmt(p.visibility,0,' m')}</td></tr>
              <tr><td style="color:#94a3b8;">🌊 Current</td><td style="text-align:right;">${fmt(p.current_vel,2,' m/s')} @ ${fmt(p.current_dir,0,'°')}</td></tr>
            </table>
            <div style="margin-top:4px;font-size:10px;color:#64748b;">${p.routeLabel || ''}</div>
          </div>`).addTo(map);
        if (hoveredWaypointId !== null) {
          map.setFeatureState({ source: 'route-waypoints', id: hoveredWaypointId }, { hover: false });
        }
        hoveredWaypointId = f.id;
        map.setFeatureState({ source: 'route-waypoints', id: hoveredWaypointId }, { hover: true });
      });
      map.on('mouseleave', 'route-waypoints-hit', () => {
        map.getCanvas().style.cursor = '';
        waypointPopup.remove();
        if (hoveredWaypointId !== null) {
          map.setFeatureState({ source: 'route-waypoints', id: hoveredWaypointId }, { hover: false });
          hoveredWaypointId = null;
        }
      });
    });
    return () => map.remove();
  }, []);
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.getSource('routes')) return;
    const features = [];
    if (astarcoordinates.length > 1) {
      features.push({ type: 'Feature', properties: { routeType: 'astar' },
        geometry: { type: 'LineString', coordinates: astarcoordinates.map((c) => [c[1], c[0]]) } });
    }
    if (coordinates.length > 1) {
      features.push({ type: 'Feature', properties: { routeType: 'optimized' },
        geometry: { type: 'LineString', coordinates: coordinates.map((c) => [c[1], c[0]]) } });
      const last = coordinates[coordinates.length - 1];
      map.flyTo({ center: [last[1], last[0]], zoom: Math.max(map.getZoom(), 4) });
    }
    map.getSource('routes').setData({ type: 'FeatureCollection', features });
    coordsRef.current      = coordinates;
    astarCoordsRef.current = astarcoordinates;
    if (coordinates.length > 1 && astarcoordinates.length > 1) setShipAnimating(true);
  }, [astarcoordinates, coordinates]);
  useEffect(() => {
    let rafId = null;
    let lastUpdateMs = 0;
    const SHIP_PERIOD_MS = 35000;
    const animate = (timestamp) => {
      const map = mapRef.current;
      if (map && timestamp - lastUpdateMs > 110) {
        if (shipAnimatingRef.current) {
          const t = (timestamp % SHIP_PERIOD_MS) / SHIP_PERIOD_MS;
          const optPos = interpolateRoutePos(coordsRef.current, t);
          if (optPos && map.getSource('ship-opt')) {
            const bearing = routeBearingAt(coordsRef.current, t);
            map.getSource('ship-opt').setData({ type: 'FeatureCollection',
              features: [{ type: 'Feature', properties: { bearing }, geometry: { type: 'Point', coordinates: optPos } }] });
          }
          const astarPos = interpolateRoutePos(astarCoordsRef.current, t);
          if (astarPos && map.getSource('ship-astar')) {
            const bearing = routeBearingAt(astarCoordsRef.current, t);
            map.getSource('ship-astar').setData({ type: 'FeatureCollection',
              features: [{ type: 'Feature', properties: { bearing }, geometry: { type: 'Point', coordinates: astarPos } }] });
          }
        }
        lastUpdateMs  = timestamp;
      }
      rafId = requestAnimationFrame(animate);
    };
    rafId = requestAnimationFrame(animate);
    return () => { if (rafId) cancelAnimationFrame(rafId); };
  }, []);
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const vis = showRouteDots ? 'visible' : 'none';
    ['route-waypoints-dot', 'route-waypoints-hit'].forEach((id) => {
      if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis);
    });
  }, [showRouteDots]);
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    if (map.getSource('weather-raster')) {
      if (!severityData || severityData.length === 0) {
        map.getSource('weather-raster').updateImage({
          url: TRANSPARENT_1PX, coordinates: [[-180, 85], [180, 85], [180, -85], [-180, -85]],
        });
      } else {
        const raster = buildWeatherRasterCanvas(severityData);
        if (raster) map.getSource('weather-raster').updateImage({ url: raster.dataUrl, coordinates: raster.coords });
      }
    }
    if (map.getSource('severity-circles')) {
      map.getSource('severity-circles').setData({
        type: 'FeatureCollection',
        features: (severityData || [])
          .filter((p) => Array.isArray(p?.coordinate) && p.coordinate.length === 2)
          .map((p) => ({
            type: 'Feature',
            properties: { risk: clampValue(Number(p.risk) || 0, 0, 100) },
            geometry: { type: 'Point', coordinates: [Number(p.coordinate[1]), Number(p.coordinate[0])] },
          })),
      });
    }
  }, [severityData]);
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !map.getSource('risk-segments')) return;
    const buildSegments = (routeCoords, weatherPts, routeType) => {
      if (routeCoords.length < 2 || !weatherPts.length) return [];
      const riskValues = buildRouteRiskPoints(weatherPts).map((p) => p.value);
      return routeCoords.slice(0, -1).map((coord, i) => {
        const t    = i / (routeCoords.length - 1);
        const risk = interpolateSeriesValue(riskValues, t) ?? 0;
        return {
          type: 'Feature',
          properties: { routeType, risk, color: riskToColor(risk) },
          geometry: { type: 'LineString',
            coordinates: [[coord[1], coord[0]], [routeCoords[i + 1][1], routeCoords[i + 1][0]]] },
        };
      });
    };
    map.getSource('risk-segments').setData({ type: 'FeatureCollection', features: [
      ...buildSegments(coordinates,      optimizedWeather, 'optimized'),
      ...buildSegments(astarcoordinates, astarWeather,     'astar'),
    ]});
    if (map.getSource('route-waypoints')) {
      let wpId = 0;
      const buildWaypoints = (weatherPts, routeColor, routeLabel) =>
        (weatherPts || [])
          .filter((p) => Array.isArray(p?.coordinate) && p.coordinate.length === 2)
          .map((p) => {
            const risk = p.risk != null ? Number(p.risk) : computeWeatherRiskFromPoint(p);
            return {
              id: wpId++, type: 'Feature',
              properties: {
                lat: Number(p.coordinate[0]), lon: Number(p.coordinate[1]),
                wind_speed: p.wind_speed ?? null, wind_direction: p.wind_direction ?? null,
                precipitation: p.precipitation ?? null, visibility: p.visibility ?? null,
                wave_height: p.wave_height ?? null, wave_dir: p.wave_dir ?? null,
                current_vel: p.current_vel ?? null, current_dir: p.current_dir ?? null,
                risk: Math.round(risk), routeColor, routeLabel,
              },
              geometry: { type: 'Point', coordinates: [Number(p.coordinate[1]), Number(p.coordinate[0])] },
            };
          });
      map.getSource('route-waypoints').setData({ type: 'FeatureCollection', features: [
        ...buildWaypoints(optimizedWeather, '#00e676', 'Optimized route'),
        ...buildWaypoints(astarWeather,     '#ffd060', 'Baseline (A*) route'),
      ]});
    }
  }, [coordinates, astarcoordinates, optimizedWeather, astarWeather]);
  const connectWebSocket = useCallback(() => {
    setProgress({ pct: 0, step: 'Connecting…' });
    setStatus('Connecting...');
    wsRef.current = new WebSocket('ws://localhost:5000');
    wsRef.current.onopen = () => {
      setProgress({ pct: 3, step: 'Preparing request…' });
      setStatus('Calculating Routes...');
      setReportUrl(null);
      setExcelUrl(null);
      wsRef.current.send(JSON.stringify({ type: 'start',
        start: start.split(',').map(Number),
        end:   end.split(',').map(Number),
        mode,
      }));
    };
    wsRef.current.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'progress') {
        setProgress({ pct: data.pct, step: data.step, eta_sec: data.eta_sec });
      } else if (data.type === 'final') {
        setProgress({ pct: 100, step: 'Complete!' });
        setCoordinates(data.path || []);
        setAstarCoordinates(data.apath || []);
        setSeverityData(data.severity || []);
        setOptimizedWeather(data.optimized_weather || data.weather || []);
        setAstarWeather(data.astar_weather || []);
        setMetrics(data.metrics || null);
        setDistance(data.distance || 0);
        setStatus('Routes Calculated');
        setShowMetrics(true);
        setTimeout(() => setProgress({ pct: 0, step: '' }), 2000);
      } else if (data.type === 'report_ready') {
        setReportUrl(data.report_url);
      } else if (data.type === 'excel_ready') {
        setExcelUrl(data.excel_url);
      } else if (data.type === 'error') {
        setProgress({ pct: 0, step: '' });
        setStatus(`Error: ${data.message}`);
      }
    };
    wsRef.current.onerror = () => setStatus('Connection Error');
    wsRef.current.onclose = () => setStatus('Disconnected');
  }, [start, end, mode]);
  const startNavigation = () => {
    if (!start || !end) {
      setCoordWarning('⚠ Please set both start and end points on the map before navigating.');
      return;
    }
    setCoordWarning('');
    connectWebSocket();
  };
  const stopNavigation = () => {
    if (wsRef.current) { wsRef.current.close(); wsRef.current = null; }
    setStatus('Stopped');
  };
  const isCalculating = status.startsWith('Calculating');
  return (
    <div style={{ height: '100vh', width: '100vw', overflow: 'hidden', position: 'relative' }}>
      <div ref={mapContainerRef} style={{ width: '100%', height: '100%' }} />
      {}
      <div style={headerStyle}>
        AI Ship Navigation System
      </div>
      {}
      <div style={panelStyle}>
        <div style={{ marginBottom: 10, fontSize: 13, display: 'flex', alignItems: 'center', gap: 8 }}>
          {isCalculating && <span style={spinnerStyle} />}
          Status: <strong style={{ color: isCalculating ? '#ffd060' : '#fff' }}>{status}</strong>
        </div>
        {progress.pct > 0 && (
          <div style={{ marginBottom: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11,
              color: 'rgba(200,220,255,0.7)', marginBottom: 5 }}>
              <span style={{ maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {progress.step || 'Initialising…'}
              </span>
              <span style={{ 
                fontWeight: 700, 
                color: progress.pct === 100 ? '#4eff9a' : '#ffd060',
                whiteSpace: 'nowrap',
                display: 'flex',
                alignItems: 'baseline',
                gap: 8,
                flexShrink: 0
              }}>
                <span>{progress.pct}%</span>
              </span>
            </div>
            <div style={{ height: 6, borderRadius: 4, background: 'rgba(255,255,255,0.08)',
              overflow: 'hidden', border: '1px solid rgba(100,150,255,0.2)' }}>
              <div style={{
                height: '100%', borderRadius: 4,
                width: `${progress.pct}%`,
                background: progress.pct === 100
                  ? 'linear-gradient(90deg, #4eff9a, #00c67a)'
                  : 'linear-gradient(90deg, #3b7fff, #9ab4ff)',
                transition: 'width 0.4s ease',
              }} />
            </div>
          </div>
        )}
        {metrics && (
          <div style={pitchCalloutStyle}>
            <div style={pitchCalloutTitleStyle}>Optimization Impact</div>
            <div style={pitchCalloutValuesStyle}>
              {(() => {
                const riskD = (metrics.astar?.risk_score ?? 0) - (metrics.optimized?.risk_score ?? 0);
                const fuelD = metrics.fuel_tonnes_saved ?? 0;
                const etaD  = metrics.eta_hours_saved  ?? 0;
                const fmt   = (v, unit, saveLabel, riseLabel) => {
                  const abs = Math.abs(v).toFixed(1);
                  if (v > 0.05)  return <span style={{ color: '#4eff9a' }}>{saveLabel} {abs} {unit}</span>;
                  if (v < -0.05) return <span style={{ color: '#ff6060' }}>{riseLabel} {abs} {unit}</span>;
                  return <span style={{ color: '#aaa' }}>No change</span>;
                };
                return (<>
                  {fmt(riskD, 'pts', 'Risk ↓', 'Risk ↑')}
                  {fmt(fuelD, 't',   'Fuel ↓', 'Fuel ↑')}
                  {fmt(etaD,  'h',   'Time ↓', 'Time ↑')}
                </>);
              })()}
            </div>
          </div>
        )}
        {}
        <div style={{ display: 'flex', gap: 0, marginBottom: 10, borderRadius: 8, overflow: 'hidden', border: '1px solid rgba(255,255,255,0.15)' }}>
          {['map', 'manual'].map(m => (
            <button key={m} onClick={() => setInputMode(m)} style={{
              flex: 1, padding: '7px 0', border: 'none', cursor: 'pointer', fontSize: 11, fontWeight: 600,
              background: inputMode === m ? 'rgba(255,255,255,0.18)' : 'transparent',
              color:      inputMode === m ? '#fff'                   : 'rgba(255,255,255,0.45)',
            }}>
              {m === 'map' ? 'Pick on Map' : 'Type Coords'}
            </button>
          ))}
        </div>
        {inputMode === 'map' ? (
          <div style={{ display: 'flex', gap: 8, marginBottom: 8 }}>
            <button
              onClick={() => { clearRouteState(); setPickMode(pickMode === 'start' ? null : 'start'); }}
              style={{
                flex: 1, padding: '10px 6px', borderRadius: 8, border: 'none',
                cursor: 'pointer', fontWeight: 600, fontSize: 12,
                background: pickMode === 'start' ? '#00e676' : 'rgba(0,230,118,0.15)',
                color:      pickMode === 'start' ? '#000'    : '#00e676',
              }}>
              {pickMode === 'start' ? 'Click map...' : 'Set Start'}
            </button>
            <button
              onClick={() => { clearRouteState(); setPickMode(pickMode === 'end' ? null : 'end'); }}
              style={{
                flex: 1, padding: '10px 6px', borderRadius: 8, border: 'none',
                cursor: 'pointer', fontWeight: 600, fontSize: 12,
                background: pickMode === 'end' ? '#ff4444' : 'rgba(255,68,68,0.15)',
                color:      pickMode === 'end' ? '#fff'    : '#ff9a9a',
              }}>
              {pickMode === 'end' ? 'Click map...' : 'Set End'}
            </button>
          </div>
        ) : (
          <>
            <input placeholder="Start: lat,lon  e.g. 40.6,-74.0" value={manualStart}
              onChange={e => setManualStart(e.target.value)}
              style={{ ...inputStyle, width: '100%', marginBottom: 6 }} />
            <input placeholder="End: lat,lon  e.g. 50.9,-1.4" value={manualEnd}
              onChange={e => setManualEnd(e.target.value)}
              style={{ ...inputStyle, width: '100%', marginBottom: 6 }} />
            <button onClick={applyManualCoords} style={{
              width: '100%', padding: '8px', borderRadius: 8, border: 'none',
              background: 'rgba(255,255,255,0.12)', color: '#fff',
              cursor: 'pointer', fontSize: 12, fontWeight: 600, marginBottom: 8,
            }}>
              Apply Coordinates
            </button>
          </>
        )}
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, marginBottom: 6, opacity: 0.8 }}>
          <span style={{ color: '#00e676' }}>{start || 'Start not set'}</span>
          <span style={{ color: '#ff9a9a' }}>{end   || 'End not set'}</span>
        </div>
        {coordWarning && (
          <div style={{
            fontSize: 11, color: '#ffb347', background: 'rgba(255,140,0,0.12)',
            border: '1px solid rgba(255,140,0,0.35)', borderRadius: 7,
            padding: '7px 10px', marginBottom: 8, lineHeight: 1.5,
          }}>
            {coordWarning}
          </div>
        )}
        <select value={mode} onChange={(e) => setMode(e.target.value)}
          style={{ ...inputStyle, width: '100%', color: '#fff', backgroundColor: 'rgba(25,30,40,0.98)' }}>
          <option value="balanced"         style={selectOptionStyle}>Balanced</option>
          <option value="safety"           style={selectOptionStyle}>Safety (lowest risk)</option>
          <option value="distance"         style={selectOptionStyle}>Distance (fastest A*)</option>
          <option value="fuel consumption" style={selectOptionStyle}>Fuel Efficient</option>
        </select>
        <div style={{ display: 'flex', gap: 8 }}>
          <button onClick={startNavigation}
            style={{ ...buttonStyle, flex: 1, opacity: isCalculating ? 0.6 : 1 }}
            disabled={isCalculating}>
            {isCalculating ? 'Calculating…' : 'Start Navigation'}
          </button>
          {isCalculating && (
            <button onClick={stopNavigation} style={{
              padding: '12px 14px', borderRadius: 8, border: 'none',
              background: 'rgba(220,50,50,0.55)', color: '#fff',
              cursor: 'pointer', fontWeight: 'bold', fontSize: 13,
            }}>■</button>
          )}
        </div>
        <div style={distanceCardStyle}>
          Total Distance: <strong>{Number(distance).toFixed(0)} km</strong>
        </div>
        <div style={legendRowStyle}>
          <span style={{ ...legendDotStyle, background: '#ffd060' }} />
          <span style={{ fontSize: 11, color: '#ffd060', fontWeight: 600 }}>A* Baseline</span>
          <span style={{ ...legendDotStyle, background: '#00e676', marginLeft: 14 }} />
          <span style={{ fontSize: 11, color: '#00e676', fontWeight: 600 }}>Optimised</span>
        </div>
        {coordinates.length > 1 && (
          <button onClick={() => setShipAnimating(v => !v)} style={{
            marginTop: 8, width: '100%', padding: '7px',
            background: shipAnimating ? 'rgba(0,255,170,0.12)' : 'rgba(255,255,255,0.07)',
            color:      shipAnimating ? '#00ffaa'               : 'rgba(255,255,255,0.55)',
            border: `1px solid ${shipAnimating ? 'rgba(0,255,170,0.4)' : 'rgba(255,255,255,0.15)'}`,
            borderRadius: 8, cursor: 'pointer', fontSize: 11, fontWeight: 600,
          }}>
            {shipAnimating ? 'Pause Ship Simulation' : 'Play Ship Simulation'}
          </button>
        )}
        <button onClick={() => setShowRouteDots((v) => !v)} style={{
          marginTop: 8, width: '100%', padding: '7px',
          background: showRouteDots ? 'rgba(100,180,255,0.12)' : 'rgba(255,255,255,0.07)',
          color:      showRouteDots ? '#9ab4ff'                : 'rgba(255,255,255,0.55)',
          border: `1px solid ${showRouteDots ? 'rgba(100,180,255,0.4)' : 'rgba(255,255,255,0.15)'}`,
          borderRadius: 8, cursor: 'pointer', fontSize: 11, fontWeight: 600,
        }}>
          {showRouteDots ? 'Hide Route Dots' : 'Show Route Dots'}
        </button>
        {metrics && (
          <button onClick={() => setShowMetrics((v) => !v)} style={toggleMetricsStyle}>
            {showMetrics ? 'Hide Metrics' : 'Show Metrics'}
          </button>
        )}
      </div>
      {showMetrics && metrics && <MetricsPanel metrics={metrics} mode={mode} />}
      {metrics && <ResultBanner metrics={metrics} reportUrl={reportUrl} excelUrl={excelUrl} />}
      {}
      <div style={legendDockStyle}>
        <div style={legendBarStyle}>
          <span style={legendUnitStyle}>risk</span>
          <div style={legendGradientStyle} />
          <div style={legendTicksStyle}>
            {['Calm', 'Low', 'Mod', 'High', 'Severe', 'Extreme'].map((t) => (
              <span key={t}>{t}</span>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
export default App;