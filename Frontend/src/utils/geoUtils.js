const WATER_TYPES = new Set([
  'ocean', 'sea', 'bay', 'strait', 'channel', 'harbour', 'harbor',
  'water', 'lake', 'river', 'reservoir', 'fjord', 'sound', 'cove', 'lagoon',
]);
export async function isWaterCoordinate(lat, lon) {
  try {
    const resp = await fetch(
      `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lon}&format=json`,
      { headers: { 'Accept-Language': 'en' } },
    );
    const data = await resp.json();
    if (data.error) return true;
    const type = (data.type || '').toLowerCase();
    return WATER_TYPES.has(type);
  } catch {
    return true; // fail open — don't block the user if Nominatim is down
  }
}

function segmentDist(a, b) {
  const toRad = d => d * Math.PI / 180;
  const lat1 = toRad(a[0]), lat2 = toRad(b[0]);
  const dLat = lat2 - lat1;
  const dLon = toRad(b[1] - a[1]);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h)); 
}
export function interpolateRoutePos(coords, t) {
  if (!coords || coords.length < 2) return null;
  const clamped = Math.min(1, Math.max(0, t));
  const cum = [0];
  for (let i = 1; i < coords.length; i++) {
    cum.push(cum[i - 1] + segmentDist(coords[i - 1], coords[i]));
  }
  const total = cum[cum.length - 1];
  if (total === 0) return [coords[0][1], coords[0][0]];
  const target = clamped * total;
  let lo = 0, hi = cum.length - 2;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (cum[mid + 1] < target) lo = mid + 1; else hi = mid;
  }
  const i = lo;
  const segLen = cum[i + 1] - cum[i];
  const f = segLen > 0 ? (target - cum[i]) / segLen : 0;
  const a = coords[i];
  const b = coords[i + 1];
  return [a[1] + f * (b[1] - a[1]), a[0] + f * (b[0] - a[0])];
}
function bearingBetween(a, b) {
  if (!a || !b) return 0;
  const toRad = d => d * Math.PI / 180;
  const lat1 = toRad(a[0]), lat2 = toRad(b[0]);
  const dLon = toRad(b[1] - a[1]);
  const x = Math.sin(dLon) * Math.cos(lat2);
  const y = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon);
  return (Math.atan2(x, y) * 180 / Math.PI + 360) % 360;
}
export function routeBearingAt(coords, t) {
  if (!coords || coords.length < 2) return 0;
  const clamped = Math.min(0.9999, Math.max(0, t));
  const cum = [0];
  for (let i = 1; i < coords.length; i++) {
    cum.push(cum[i - 1] + segmentDist(coords[i - 1], coords[i]));
  }
  const total = cum[cum.length - 1];
  if (total === 0) return 0;
  const target = clamped * total;
  let lo = 0, hi = cum.length - 2;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (cum[mid + 1] < target) lo = mid + 1; else hi = mid;
  }
  return bearingBetween(coords[lo], coords[lo + 1]);
}