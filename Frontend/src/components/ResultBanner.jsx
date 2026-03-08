function BannerPill({ icon, label, value, color }) {
  return (
    <div style={{
      background: 'rgba(8,18,38,0.93)', backdropFilter: 'blur(20px)',
      border: `1px solid ${color}55`, borderRadius: 14,
      padding: '10px 18px', textAlign: 'center', minWidth: 110,
      boxShadow: `0 0 20px ${color}22`,
    }}>
      <div style={{ fontSize: 18 }}>{icon}</div>
      <div style={{ fontSize: 10, color: 'rgba(200,220,255,0.6)', marginTop: 2 }}>{label}</div>
      <div style={{ fontSize: 17, fontWeight: 800, color, marginTop: 1 }}>{value}</div>
    </div>
  );
}
export default function ResultBanner({ metrics, reportUrl, excelUrl }) {
  if (!metrics?.optimized || !metrics?.astar) return null;
  const riskDelta   = metrics.astar.risk_score - metrics.optimized.risk_score;
  const fuelSaved   = metrics.fuel_tonnes_saved ?? 0;
  const etaSaved    = metrics.eta_hours_saved   ?? 0;
  const distSaved   = metrics.distance_saved_km ?? 0;
  
  const fmtEta = (h) => {
    const abs = Math.abs(h);
    if (!abs || isNaN(abs)) return null;
    const d = Math.floor(abs / 24), hr = Math.round(abs % 24);
    return d > 0 ? `${d}d ${hr}h` : `${hr}h`;
  };

  const fmtEtaCell = (h) => {
    if (h == null || isNaN(h)) return '—';
    const d = Math.floor(Math.abs(h) / 24), hr = Math.round(Math.abs(h) % 24);
    return d > 0 ? `${d}d ${hr}h` : `${hr}h`;
  };

  const etaDisplayDiffers = fmtEtaCell(metrics.astar.eta_hours) !== fmtEtaCell(metrics.optimized.eta_hours);
  const distDisplayDiffers = Math.round(Math.abs(distSaved)) > 0;
  const riskDisplayDiffers = Math.abs(riskDelta) >= 0.05 &&
    Number(metrics.astar.risk_score).toFixed(1) !== Number(metrics.optimized.risk_score).toFixed(1);

  const btnStyle = {
    textDecoration: 'none',
    color: '#000',
    padding: '10px 22px',
    borderRadius: 20,
    fontWeight: 800,
    fontSize: 12,
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    transition: 'all 0.25s cubic-bezier(0.4, 0, 0.2, 1)',
    pointerEvents: 'auto',
    letterSpacing: '0.02em'
  };

  return (
    <div style={{
      position: 'absolute', bottom: 60, left: '50%', transform: 'translateX(-50%)',
      display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: 12, zIndex: 20, 
      pointerEvents: 'none', width: '90%', maxWidth: '1000px',
      animation: 'slideUp 0.5s ease-out',
    }}>
      {riskDisplayDiffers && riskDelta > 0 && (
        <BannerPill icon="🛡" label="Risk reduced"     value={`${riskDelta.toFixed(1)} pts`}              color="#4eff9a" />
      )}
      {riskDisplayDiffers && riskDelta < 0 && (
        <BannerPill icon="⚠" label="Risk increased"   value={`${Math.abs(riskDelta).toFixed(1)} pts`}    color="#ff6060" />
      )}
      {fuelSaved > 0.05 && (
        <BannerPill icon="⛽" label="Fuel saved"       value={`${fuelSaved.toFixed(1)} t`}                color="#ffd060" />
      )}
      {fuelSaved < -0.05 && (
        <BannerPill icon="⛽" label="Fuel increased"   value={`${Math.abs(fuelSaved).toFixed(1)} t`}      color="#ff6060" />
      )}
      {etaDisplayDiffers && etaSaved > 0.05 && fmtEta(etaSaved) && (
        <BannerPill icon="⏱" label="Time saved"       value={fmtEta(etaSaved)}                           color="#9ab4ff" />
      )}
      {etaDisplayDiffers && etaSaved < -0.05 && fmtEta(etaSaved) && (
        <BannerPill icon="⏱" label="Time increased"   value={fmtEta(etaSaved)}                           color="#ff9060" />
      )}
      {distDisplayDiffers && distSaved > 0.5 && (
        <BannerPill icon="📏" label="Distance saved"  value={`${Math.round(distSaved)} km`}              color="#4eff9a" />
      )}
      {distDisplayDiffers && distSaved < -0.5 && (
        <BannerPill icon="📏" label="Distance added"  value={`${Math.round(Math.abs(distSaved))} km`}    color="#ffaa40" />
      )}

      {(excelUrl || reportUrl) && (
        <div style={{ pointerEvents: 'none', display: 'flex', gap: 10, alignItems: 'center', marginTop: 8 }}>
          {excelUrl && (
             <a 
               href={`http://localhost:5000${excelUrl}`} 
               download="Voyage_Weather_Log.xlsx"
               style={{
                 ...btnStyle,
                 background: 'linear-gradient(135deg, #00C9FF 0%, #92FE9D 100%)',
                 boxShadow: '0 4px 15px rgba(0, 201, 255, 0.3)',
               }}
               onMouseOver={e => {
                 e.currentTarget.style.transform = 'translateY(-3px)';
                 e.currentTarget.style.boxShadow = '0 8px 25px rgba(0, 201, 255, 0.4)';
               }}
               onMouseOut={e => {
                 e.currentTarget.style.transform = 'translateY(0)';
                 e.currentTarget.style.boxShadow = '0 4px 15px rgba(0, 201, 255, 0.3)';
               }}
             >
               <span>📊</span> WEATHER LOG (XLSX)
             </a>
          )}

          {reportUrl && (
            <a 
              href={`http://localhost:5000${reportUrl}`} 
              target="_blank" 
              rel="noopener noreferrer"
              style={{
                ...btnStyle,
                background: 'linear-gradient(135deg, #FFD700 0%, #FFA500 100%)',
                boxShadow: '0 4px 15px rgba(255, 215, 0, 0.4)',
              }}
              onMouseOver={e => {
                e.currentTarget.style.transform = 'translateY(-3px)';
                e.currentTarget.style.boxShadow = '0 8px 25px rgba(255, 215, 0, 0.6)';
              }}
              onMouseOut={e => {
                e.currentTarget.style.transform = 'translateY(0)';
                e.currentTarget.style.boxShadow = '0 4px 15px rgba(255, 215, 0, 0.4)';
              }}
            >
              <span>📄</span> VOYAGE REPORT (PDF)
            </a>
          )}
        </div>
      )}
    </div>
  );
}