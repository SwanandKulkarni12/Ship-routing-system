import { GRID_SPACING } from '../constants';
const toFiniteNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};
export const clampValue = (value, lo, hi) => Math.min(hi, Math.max(lo, value));
const RISK_STOPS_RGB = [
  [0.00, [ 10,  30, 100]],
  [0.15, [ 20,  80, 200]],
  [0.30, [ 30, 160, 230]],
  [0.45, [ 40, 210, 180]],
  [0.58, [120, 220,  80]],
  [0.68, [240, 230,  30]],
  [0.78, [255, 140,   0]],
  [0.88, [230,  30, 120]],
  [1.00, [130,   0, 180]],
];
export const RISK_LEGEND = `linear-gradient(90deg, ${
  RISK_STOPS_RGB.map(([p, [r, g, b]]) => `rgb(${r},${g},${b}) ${p * 100}%`).join(', ')
})`;
function interpolateRiskRGB(t) {
  const stops = RISK_STOPS_RGB;
  let lo = stops[0], hi = stops[stops.length - 1];
  for (let i = 0; i < stops.length - 1; i++) {
    if (t >= stops[i][0] && t <= stops[i + 1][0]) {
      lo = stops[i]; hi = stops[i + 1];
      break;
    }
  }
  const f = (t - lo[0]) / (hi[0] - lo[0] || 1);
  return [
    Math.round(lo[1][0] + f * (hi[1][0] - lo[1][0])),
    Math.round(lo[1][1] + f * (hi[1][1] - lo[1][1])),
    Math.round(lo[1][2] + f * (hi[1][2] - lo[1][2])),
  ];
}
export function riskToColor(risk) {
  const [r, g, b] = interpolateRiskRGB(clampValue(risk, 0, 100) / 100);
  return `rgb(${r},${g},${b})`;
}
export function computeWeatherRiskFromPoint(point) {
  const wind = toFiniteNumber(point?.wind_speed)    ?? 0.0;
  const wave = toFiniteNumber(point?.wave_height)   ?? 0.0;
  const rain = toFiniteNumber(point?.precipitation) ?? 0.0;
  const vis  = toFiniteNumber(point?.visibility)    ?? 10000.0;
  const waveSev   = clampValue(wave / 4.0,          0.0, 1.0);
  const windSev   = clampValue(wind / 35.0,         0.0, 1.0);
  const precipSev = clampValue(rain / 10.0,         0.0, 1.0);
  const visRisk   = clampValue(1.0 - vis / 10000.0, 0.0, 1.0);
  return (0.40 * waveSev + 0.30 * windSev + 0.20 * visRisk + 0.10 * precipSev) * 100.0;
}
export function buildRouteRiskPoints(weatherPoints) {
  return (weatherPoints || [])
    .filter((p) => Array.isArray(p?.coordinate) && p.coordinate.length === 2)
    .map((p) => ({
      lat:   Number(p.coordinate[0]),
      lon:   Number(p.coordinate[1]),
      value: computeWeatherRiskFromPoint(p),
    }))
    .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lon) && Number.isFinite(p.value));
}
export function interpolateSeriesValue(values, t) {
  if (!values.length) return null;
  if (values.length === 1) return values[0];
  const scaled = clampValue(t, 0, 1) * (values.length - 1);
  const i0  = Math.floor(scaled);
  const i1  = Math.min(values.length - 1, i0 + 1);
  const mix = scaled - i0;
  return values[i0] * (1 - mix) + values[i1] * mix;
}
export function buildWeatherRasterCanvas(severityPoints) {
  if (!severityPoints || severityPoints.length < 4) return null;
  const pts = severityPoints
    .filter((p) => Array.isArray(p?.coordinate) && p.coordinate.length === 2)
    .map((p) => ({
      lat:  Number(p.coordinate[0]),
      lon:  Number(p.coordinate[1]),
      risk: clampValue(Number(p.risk) || 0, 0, 100),
    }))
    .filter((p) => isFinite(p.lat) && isFinite(p.lon));
  if (pts.length < 4) return null;
  let minLat = Infinity, maxLat = -Infinity, minLon = Infinity, maxLon = -Infinity;
  for (const { lat, lon } of pts) {
    if (lat < minLat) minLat = lat; if (lat > maxLat) maxLat = lat;
    if (lon < minLon) minLon = lon; if (lon > maxLon) maxLon = lon;
  }
  const padLat = Math.max((maxLat - minLat) * 0.35, GRID_SPACING * 4);
  const padLon = Math.max((maxLon - minLon) * 0.35, GRID_SPACING * 4);
  const bMinLat = minLat - padLat, bMaxLat = maxLat + padLat;
  const bMinLon = minLon - padLon, bMaxLon = maxLon + padLon;
  const bLatSpan = bMaxLat - bMinLat || 1;
  const bLonSpan = bMaxLon - bMinLon || 1;
  const W = 512, H = 512;
  const riskAcc   = new Float32Array(W * H);
  const weightAcc = new Float32Array(W * H);
  const splatDeg = GRID_SPACING * 5.0;
  const splatPx  = Math.max(
    (splatDeg / bLonSpan) * W,
    (splatDeg / bLatSpan) * H,
    12,                          
  );
  const sigma = splatPx * 0.6;  
  const sig2  = 2 * sigma * sigma;
  const r2    = splatPx * splatPx;
  for (const p of pts) {
    const cx = ((p.lon - bMinLon) / bLonSpan) * W;
    const cy = ((bMaxLat - p.lat) / bLatSpan) * H;
    const x0 = Math.max(0,     Math.floor(cx - splatPx));
    const x1 = Math.min(W - 1, Math.ceil(cx  + splatPx));
    const y0 = Math.max(0,     Math.floor(cy - splatPx));
    const y1 = Math.min(H - 1, Math.ceil(cy  + splatPx));
    for (let py = y0; py <= y1; py++) {
      const dy  = py - cy;
      const dy2 = dy * dy;
      for (let px = x0; px <= x1; px++) {
        const dx = px - cx;
        const d2 = dx * dx + dy2;
        if (d2 > r2) continue;
        const w = Math.exp(-d2 / sig2);
        const idx = py * W + px;
        riskAcc[idx]   += w * p.risk;
        weightAcc[idx] += w;
      }
    }
  }
  const canvas = document.createElement('canvas');
  canvas.width = W; canvas.height = H;
  const ctx = canvas.getContext('2d');
  const img = ctx.createImageData(W, H);
  const d   = img.data;
  const [bgR, bgG, bgB] = interpolateRiskRGB(0.03); 
  const BG_ALPHA = 22;  
  for (let i = 0; i < W * H; i++) {
    const wt = weightAcc[i];
    if (wt < 1e-6) {
      d[i * 4]     = bgR;
      d[i * 4 + 1] = bgG;
      d[i * 4 + 2] = bgB;
      d[i * 4 + 3] = BG_ALPHA;
    } else {
      const risk = riskAcc[i] / wt;
      const alpha = Math.max(Math.round((1 - Math.exp(-wt * 4)) * 200), BG_ALPHA);
      const [r, g, b] = interpolateRiskRGB(risk / 100);
      d[i * 4]     = r;
      d[i * 4 + 1] = g;
      d[i * 4 + 2] = b;
      d[i * 4 + 3] = alpha;
    }
  }
  ctx.putImageData(img, 0, 0);
  const blurPx = Math.max(Math.round(splatPx * 0.2), 4);
  const out = document.createElement('canvas');
  out.width = W; out.height = H;
  const outCtx = out.getContext('2d');
  outCtx.filter = `blur(${blurPx}px)`;
  outCtx.drawImage(canvas, 0, 0);
  return {
    dataUrl: out.toDataURL('image/png'),
    coords: [
      [bMinLon, bMaxLat],
      [bMaxLon, bMaxLat],
      [bMaxLon, bMinLat],
      [bMinLon, bMinLat],
    ],
  };
}