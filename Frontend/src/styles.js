import { RISK_LEGEND } from './utils/riskUtils';
export const headerStyle = {
  position: 'absolute', top: 0, left: 0, width: '100%',
  padding: '14px 20px', color: '#fff',
  background: 'rgba(5, 15, 32, 0.82)', backdropFilter: 'blur(20px)',
  fontSize: '20px', fontWeight: 'bold', zIndex: 10,
};
export const panelStyle = {
  position: 'absolute', top: '60px', left: '20px', width: '310px',
  background: 'rgba(10, 20, 40, 0.92)', backdropFilter: 'blur(22px)',
  border: '1px solid rgba(255,255,255,0.15)', borderRadius: '15px',
  padding: '16px', color: '#fff', zIndex: 10, boxSizing: 'border-box',
};
export const inputStyle = {
  width: '100%', marginBottom: '10px', padding: '10px',
  borderRadius: '8px', border: '1px solid rgba(255,255,255,0.3)',
  background: 'rgba(255,255,255,0.1)', color: '#fff', outline: 'none',
  boxSizing: 'border-box',
};
export const selectOptionStyle = { color: '#fff', backgroundColor: '#1a2230' };
export const buttonStyle = {
  width: '100%', padding: '12px',
  backgroundColor: 'rgba(14,25,229,0.5)', color: '#fff',
  border: 'none', borderRadius: '8px', cursor: 'pointer', fontWeight: 'bold',
};
export const spinnerStyle = {
  width: 10, height: 10, borderRadius: '50%',
  border: '2px solid rgba(255,255,255,0.25)', borderTopColor: '#ffd060',
  animation: 'spin 0.8s linear infinite',
};
export const toggleMetricsStyle = {
  marginTop: 8, width: '100%', padding: '8px',
  background: 'rgba(255,255,255,0.08)', color: 'rgba(255,255,255,0.8)',
  border: '1px solid rgba(255,255,255,0.2)', borderRadius: '8px',
  cursor: 'pointer', fontSize: 12,
};
export const distanceCardStyle = {
  marginTop: '12px', fontSize: '14px', padding: '10px',
  borderRadius: '10px', border: '1px solid rgba(255,255,255,0.3)',
  background: 'rgba(255,255,255,0.07)', color: '#fff', textAlign: 'center',
};
export const pitchCalloutStyle = {
  border: '1px solid rgba(78,255,154,0.35)',
  background: 'linear-gradient(135deg, rgba(17,38,64,0.82), rgba(14,25,49,0.82))',
  borderRadius: 10, padding: '9px 10px', marginBottom: 10,
};
export const pitchCalloutTitleStyle = {
  fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.9,
  color: 'rgba(156,255,206,0.95)', marginBottom: 5, fontWeight: 700,
};
export const pitchCalloutValuesStyle = {
  display: 'flex', gap: 10, fontSize: 11, fontWeight: 700,
  color: '#d8e7ff', flexWrap: 'wrap',
};
export const legendRowStyle = {
  display: 'flex', alignItems: 'center', gap: 6, marginTop: 10, flexWrap: 'wrap',
};
export const legendDotStyle = {
  display: 'inline-block', width: 24, height: 3, borderRadius: 2,
};
export const legendDockStyle = { position: 'absolute', left: '14px', bottom: '14px', zIndex: 11 };
export const legendBarStyle = {
  position: 'relative', display: 'flex', alignItems: 'center', gap: '10px',
  borderRadius: '14px', padding: '6px 10px 20px 10px',
  background: 'rgba(34,51,78,0.95)', border: '1px solid rgba(180,210,255,0.35)',
  boxShadow: '0 2px 10px rgba(0,0,0,0.35)', color: '#e6f0ff', fontSize: '12px', fontWeight: 700,
};
export const legendUnitStyle = {
  background: 'rgba(0,0,0,0.18)', borderRadius: '10px',
  padding: '2px 6px', textTransform: 'lowercase',
};
export const legendGradientStyle = {
  width: '320px', height: '18px', borderRadius: '10px',
  backgroundImage: RISK_LEGEND,
  backgroundColor: '#2d3d5a',
  border: '1px solid rgba(255,255,255,0.2)',
  boxShadow: 'inset 0 0 0 1px rgba(0,0,0,0.25)',
};
export const legendTicksStyle = {
  width: '320px', display: 'flex', justifyContent: 'space-between',
  position: 'absolute', left: '56px', top: '27px',
  color: 'rgba(230,240,255,0.95)', fontSize: '10px', fontWeight: 600,
  pointerEvents: 'none',
};