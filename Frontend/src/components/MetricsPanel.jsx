const metricsPanelStyle = {
  position: 'absolute', top: '60px', right: '20px', width: '420px',
  background: 'rgba(8, 18, 38, 0.95)', backdropFilter: 'blur(24px)',
  border: '1px solid rgba(100,150,255,0.25)', borderRadius: '16px',
  padding: '18px', color: '#e8eeff', zIndex: 10,
  boxShadow: '0 8px 32px rgba(0,0,20,0.5)',
};
const metricsHeaderStyle = {
  fontSize: 15, fontWeight: 700, marginBottom: 14,
  color: '#c8d8ff', display: 'flex', alignItems: 'center',
};
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: 13 };
const thStyle = {
  padding: '6px 8px', textAlign: 'right', fontSize: 11, fontWeight: 600,
  color: 'rgba(200,220,255,0.6)', borderBottom: '1px solid rgba(255,255,255,0.1)',
};
const tdLabelStyle = {
  padding: '7px 8px', color: 'rgba(200,220,255,0.7)', fontSize: 12,
  borderBottom: '1px solid rgba(255,255,255,0.06)',
};
const tdStyle = {
  padding: '7px 8px', textAlign: 'right', fontWeight: 600,
  borderBottom: '1px solid rgba(255,255,255,0.06)',
};
const savingsSectionStyle = { display: 'flex', flexWrap: 'wrap', gap: 8, marginTop: 14 };
const savingBadgeStyle = {
  flex: '1 1 calc(50% - 8px)', padding: '8px 10px',
  borderRadius: 10, border: '1px solid',
  background: 'rgba(255,255,255,0.04)', minWidth: 110,
};
const focusTextStyle = {
  marginTop: 12, fontSize: 11, color: 'rgba(180,210,255,0.65)',
  lineHeight: 1.5, borderTop: '1px solid rgba(255,255,255,0.08)', paddingTop: 10,
};
function MetricRow({ label, a, b, delta, deltaFmt, goodIfPositive }) {
  const valuesEqual = a != null && b != null && String(a) === String(b);
  const isGood = !valuesEqual && delta != null && (goodIfPositive ? delta > 0.05  : delta < -0.05);
  const isBad  = !valuesEqual && delta != null && (goodIfPositive ? delta < -0.05 : delta > 0.05);
  return (
    <tr>
      <td style={tdLabelStyle}>{label}</td>
      <td style={{ ...tdStyle, color: '#ffd060' }}>{a}</td>
      <td style={{ ...tdStyle, color: isGood ? '#4eff9a' : isBad ? '#ff6060' : '#e8eeff' }}>{b}</td>
      <td style={{ ...tdStyle, padding: '7px 4px' }}>
        {(() => {
          if (!isGood && !isBad) return <span style={{ fontSize: 10, color: 'rgba(255,255,255,0.2)' }}>—</span>;
          const fmtVal = deltaFmt ? deltaFmt(delta) : Math.abs(delta).toFixed(1);
          if (!fmtVal || /^0(\.0+)?(h)?$/.test(fmtVal.trim())) {
            return <span style={{ fontSize: 10, color: 'rgba(255,255,255,0.2)' }}>—</span>;
          }
          return (
            <span style={{
              fontSize: 10, fontWeight: 700, padding: '2px 5px', borderRadius: 5,
              background: isGood ? 'rgba(78,255,154,0.15)' : 'rgba(255,80,80,0.15)',
              color:      isGood ? '#4eff9a'               : '#ff6060',
              whiteSpace: 'nowrap',
            }}>
              {isGood ? '▼ ' : '▲ '}{fmtVal}
            </span>
          );
        })()}
      </td>
    </tr>
  );
}
function SavingBadge({ label, value, color }) {
  return (
    <div style={{ ...savingBadgeStyle, borderColor: color + '55', color }}>
      <span style={{ fontSize: 10, opacity: 0.75, display: 'block' }}>{label}</span>
      <span style={{ fontWeight: 700, fontSize: 13 }}>{value}</span>
    </div>
  );
}
export default function MetricsPanel({ metrics, mode }) {
  if (!metrics) return null;
  const {
    astar, optimized,
    distance_saved_km, fuel_tonnes_saved, co2_tonnes_saved, eta_hours_saved,
    mode_explanation,
  } = metrics;
  const fmtNum = (v, dec = 1) => (v != null && !isNaN(v)) ? Number(v).toFixed(dec) : '—';
  const fmtEta = (h) => {
    if (h == null || isNaN(h)) return '—';
    const days = Math.floor(h / 24);
    const hrs  = Math.round(h % 24);
    return days > 0 ? `${days}d ${hrs}h` : `${hrs}h`;
  };
  const improved = (optimized?.risk_score ?? 999) < (astar?.risk_score ?? 0);
  const riskDelta = diff(astar?.risk_score,   optimized?.risk_score);
  const distDelta = diff(astar?.distance_km,  optimized?.distance_km);
  const etaDelta  = diff(astar?.eta_hours,    optimized?.eta_hours);
  const fuelDelta = diff(astar?.fuel_tonnes,  optimized?.fuel_tonnes);
  const co2Delta  = diff(astar?.co2_tonnes,   optimized?.co2_tonnes);
  const waveDelta = diff(astar?.avg_wave_m,   optimized?.avg_wave_m);
  return (
    <div style={metricsPanelStyle}>
      <div style={metricsHeaderStyle}>
        Route Comparison
        <span style={{
          marginLeft: 8, fontSize: 11, fontWeight: 600,
          padding: '2px 8px', borderRadius: 10,
          background: improved ? 'rgba(0,200,100,0.25)' : 'rgba(200,150,0,0.25)',
          color:      improved ? '#4eff9a'               : '#ffd060',
        }}>
          {mode?.toUpperCase() || 'BALANCED'}
        </span>
      </div>
      <table style={tableStyle}>
        <thead>
          <tr>
            <th style={thStyle}>Metric</th>
            <th style={{ ...thStyle, color: '#ffd060' }}>A* Baseline</th>
            <th style={{ ...thStyle, color: '#00e676' }}>Optimised</th>
            <th style={{ ...thStyle, color: 'rgba(200,220,255,0.4)', fontSize: 10 }}>Delta</th>
          </tr>
        </thead>
        <tbody>
          <MetricRow label="Distance"
            a={`${fmtNum(astar?.distance_km, 1)} km`}
            b={`${fmtNum(optimized?.distance_km, 1)} km`}
            delta={distDelta} deltaFmt={(d) => `${Math.abs(d).toFixed(1)} km`}
            goodIfPositive />
          <MetricRow label="ETA"
            a={fmtEta(astar?.eta_hours)}
            b={fmtEta(optimized?.eta_hours)}
            delta={etaDelta} deltaFmt={(d) => fmtEta(Math.abs(d))}
            goodIfPositive />
          <MetricRow label="Risk Score"
            a={fmtNum(astar?.risk_score)}
            b={fmtNum(optimized?.risk_score)}
            delta={riskDelta} deltaFmt={(d) => `${Math.abs(d).toFixed(1)} pts`}
            goodIfPositive />
          <MetricRow label="Fuel (HFO t)"
            a={fmtNum(astar?.fuel_tonnes)}
            b={fmtNum(optimized?.fuel_tonnes)}
            delta={fuelDelta} deltaFmt={(d) => `${Math.abs(d).toFixed(1)} t`}
            goodIfPositive />
          <MetricRow label="CO₂ (t)"
            a={fmtNum(astar?.co2_tonnes)}
            b={fmtNum(optimized?.co2_tonnes)}
            delta={co2Delta} deltaFmt={(d) => `${Math.abs(d).toFixed(1)} t`}
            goodIfPositive />
          <MetricRow label="Avg Wave"
            a={astar?.avg_wave_m     != null ? `${fmtNum(astar.avg_wave_m)} m`     : '—'}
            b={optimized?.avg_wave_m != null ? `${fmtNum(optimized.avg_wave_m)} m` : '—'}
            delta={waveDelta} deltaFmt={(d) => `${Math.abs(d).toFixed(2)} m`}
            goodIfPositive />
        </tbody>
      </table>
      <div style={savingsSectionStyle}>
        {fuel_tonnes_saved > 0.05 && (
          <SavingBadge label="Fuel saved"       value={`${fmtNum(fuel_tonnes_saved)} t HFO`}              color="#4eff9a" />
        )}
        {fuel_tonnes_saved < -0.05 && (
          <SavingBadge label="Fuel increased"   value={`${fmtNum(Math.abs(fuel_tonnes_saved))} t HFO`}    color="#ff6060" />
        )}
        {co2_tonnes_saved > 0.05 && (
          <SavingBadge label="CO₂ avoided"      value={`${fmtNum(co2_tonnes_saved)} t`}                   color="#4eff9a" />
        )}
        {co2_tonnes_saved < -0.05 && (
          <SavingBadge label="CO₂ increased"    value={`${fmtNum(Math.abs(co2_tonnes_saved))} t`}         color="#ff6060" />
        )}
        {distance_saved_km > 0.5 && (
          <SavingBadge label="Distance saved"   value={`${fmtNum(distance_saved_km, 0)} km`}              color="#9ab4ff" />
        )}
        {distance_saved_km < -0.5 && (
          <SavingBadge label="Distance added"   value={`${fmtNum(Math.abs(distance_saved_km), 0)} km`}    color="#ffaa40" />
        )}
        {eta_hours_saved > 0.05 && (
          <SavingBadge label="Time saved"       value={fmtEta(eta_hours_saved)}                           color="#ffd060" />
        )}
        {eta_hours_saved < -0.05 && (
          <SavingBadge label="Time increased"   value={fmtEta(Math.abs(eta_hours_saved))}                 color="#ff9060" />
        )}
      </div>
      {mode_explanation?.focus && (
        <div style={focusTextStyle}>{mode_explanation.focus}</div>
      )}
    </div>
  );
}
function diff(a, b) {
  return a != null && b != null ? a - b : null;
}