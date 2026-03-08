import os
import pandas as pd
import numpy as np
import logging
from fpdf import FPDF
from datetime import datetime
from openai import OpenAI
import time as _time

logger = logging.getLogger(__name__)

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend - safe for threads
import matplotlib.pyplot as plt

def generate_route_plot(astar_coords, optimized_coords, output_path):
    """Generates a professional route comparison plot."""
    try:
        plt.figure(figsize=(10, 6), dpi=100)
        plt.style.use('dark_background')
        
        # Extract Lat/Lon
        # Note: coords are often [lat, lon]
        if astar_coords:
            astar_lats = [c[0] for c in astar_coords]
            astar_lons = [c[1] for c in astar_coords]
            plt.plot(astar_lons, astar_lats, color='#FFD700', linestyle='--', linewidth=1.5, label='Baseline (A*)', alpha=0.7)
            
        if optimized_coords:
            opt_lats = [c[0] for c in optimized_coords]
            opt_lons = [c[1] for c in optimized_coords]
            plt.plot(opt_lons, opt_lats, color='#00E676', linewidth=2.5, label='Optimized Route', alpha=1.0)
            
            # Start/End points
            plt.scatter(opt_lons[0], opt_lats[0], color='white', s=50, edgecolors='black', zorder=5, label='Start')
            plt.scatter(opt_lons[-1], opt_lats[-1], color='red', s=50, edgecolors='black', zorder=5, label='Destination')

        plt.title('Voyage Route Visualization: Optimized vs Baseline', color='white', pad=20, fontsize=12, fontweight='bold')
        plt.xlabel('Longitude', color='gray')
        plt.ylabel('Latitude', color='gray')
        plt.grid(True, linestyle=':', alpha=0.3)
        plt.legend(facecolor='#141E30', edgecolor='white', fontsize=9)
        
        # Styling the plot area
        ax = plt.gca()
        ax.set_facecolor('#0E1626')
        for spine in ax.spines.values():
            spine.set_color('#2C3E50')
            
        plt.tight_layout()
        plt.savefig(output_path, facecolor='#141E30')
        plt.close()
        return output_path
    except Exception as e:
        logger.error(f"Failed to generate route plot: {e}")
        return None

def get_beaufort_force(kmh):
    """Converts km/h to Beaufort Scale force."""
    if kmh < 1: return 0
    if kmh <= 5: return 1
    if kmh <= 11: return 2
    if kmh <= 19: return 3
    if kmh <= 28: return 4
    if kmh <= 38: return 5
    if kmh <= 49: return 6
    if kmh <= 61: return 7
    if kmh <= 74: return 8
    if kmh <= 88: return 9
    if kmh <= 102: return 10
    if kmh <= 117: return 11
    return 12

class VoyageReport(FPDF):
    def header(self):
        # Professional Header Bar
        self.set_fill_color(20, 30, 48)
        self.rect(0, 0, 210, 45, 'F')
        
        # Title
        self.set_font('helvetica', 'B', 22)
        self.set_text_color(255, 255, 255)
        self.cell(0, 15, 'VOYAGE PERFORMANCE REPORT (VPR)', 0, 1, 'C')
        
        # Subtitle / Vessel Particulars Header
        self.set_font('helvetica', 'B', 9)
        self.set_y(18)
        self.cell(0, 10, "OFFICIAL PASSAGE PLAN & STRATEGIC BRIEFING", 0, 1, 'C')
        
        # Vessel Info Grid (White text on dark blue)
        self.set_font('helvetica', '', 8)
        self.set_y(28)
        self.cell(50, 4, " VESSEL: M/V OCEAN VOYAGER", 0, 0, 'L')
        self.cell(50, 4, " IMO: 9876543", 0, 0, 'L')
        self.cell(50, 4, " FLAG: PANAMA (PA)", 0, 0, 'L')
        self.cell(40, 4, " CALL SIGN: WXYZ", 0, 1, 'R')
        
        self.cell(50, 4, " TYPE: BULK CARRIER", 0, 0, 'L')
        self.cell(50, 4, " DWT: 82,000 MT", 0, 0, 'L')
        self.cell(50, 4, f" REF RUN: {datetime.now().strftime('%Y%m%d/%H%MZ')}", 0, 0, 'L')
        self.cell(40, 4, f" DATE: {datetime.now().strftime('%Y-%m-%d')}", 0, 1, 'R')
        
        self.ln(12)

    def footer(self):
        self.set_y(-25)
        self.set_font('helvetica', 'I', 7)
        self.set_text_color(160, 160, 160)
        
        # Regulatory Footer
        self.cell(0, 5, 'REGULATORY COMPLIANCE: IMO Resolution A.893(21) | MARPOL Annex VI | SEEMP III Compliant', 0, 1, 'C')
        self.cell(0, 5, 'This document is a generated Passsage Plan. Final authority rests with the Master as per SOLAS Regulation 34.', 0, 1, 'C')
        
        self.set_font('helvetica', 'B', 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f'Page {self.page_no()} | Confidential AI Voyage Intelligence', 0, 0, 'C')

def generate_voyage_pdf(analysis_data, ai_plan, output_path):
    pdf = VoyageReport()
    pdf.add_page()
    
    # --- PAGE 1: EXECUTIVE SUMMARY ---
    pdf.set_font('helvetica', 'B', 16)
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(0, 12, "1. VOYAGE PERFORMANCE SUMMARY", 0, 1, 'L', True)
    pdf.ln(5)
    
    pdf.set_font('helvetica', '', 11)
    
    metrics = analysis_data['summary']
    opt = metrics.get('optimized', {})
    
    # Grid layout for summary metrics
    col_width = 95
    line_h = 10
    
    pdf.set_fill_color(245, 247, 250)
    pdf.cell(col_width, line_h, f" Total Distance: {opt.get('distance_km', 0):.1f} km", 1, 0, 'L', True)
    pdf.cell(col_width, line_h, f" Total Duration: {opt.get('total_hours', 0):.1f} Hours", 1, 1, 'L', True)
    
    pdf.cell(col_width, line_h, f" Total Fuel (HFO): {opt.get('fuel_tonnes', 0):.1f} MT", 1, 0, 'L')
    pdf.cell(col_width, line_h, f" Carbon Footprint: {opt.get('co2_tonnes', 0):.1f} MT CO2", 1, 1, 'L')
    
    pdf.cell(col_width, line_h, f" Average Ship Speed: {opt.get('avg_speed_kts', 12.5):.1f} kts", 1, 0, 'L', True)
    pdf.cell(col_width, line_h, f" Operational Mode: {analysis_data.get('mode', 'Balanced').title()}", 1, 1, 'L', True)
    pdf.ln(10)

    # Economic Benchmarks
    pdf.set_font('helvetica', 'B', 14)
    pdf.cell(0, 10, "2. STRATEGIC SAVINGS (Green Corridor Metrics)", 0, 1, 'L')
    pdf.set_font('helvetica', '', 11)
    
    fuel_saved = metrics.get('fuel_tonnes_saved', 0)
    co2_saved = metrics.get('co2_tonnes_saved', 0)
    
    pdf.set_text_color(0, 100, 0) if fuel_saved >= 0 else pdf.set_text_color(200, 0, 0)
    pdf.cell(0, 8, f" Fuel {'Saved' if fuel_saved >= 0 else 'Increased'}: {abs(fuel_saved):.1f} MT", 0, 1)
    pdf.cell(0, 8, f" CO2 {'Abatement' if co2_saved >= 0 else 'Increase'}: {abs(co2_saved):.1f} MT", 0, 1)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(10)

    # Environmental section
    pdf.set_font('helvetica', 'B', 14)
    pdf.cell(0, 10, "3. ENVIRONMENTAL EXPOSURE & SAFETY", 0, 1, 'L')
    pdf.set_font('helvetica', '', 11)
    w = analysis_data['weather']
    pdf.cell(0, 8, f" Max Wave Height: {w['max_wave']:.2f}m | Avg: {w['avg_wave']:.2f}m", 0, 1)
    pdf.cell(0, 8, f" Max Wind Speed: {w['max_wind']:.1f} km/h (Beaufort Integration)", 0, 1)
    pdf.cell(0, 8, f" Route Severity Grade: {w['avg_severity']:.1f}% (Dynamic Stress Index)", 0, 1)
    
    # --- PAGE 2: ANALYTICAL BENCHMARKING ---
    pdf.add_page()
    pdf.set_font('helvetica', 'B', 16)
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(0, 12, "4. PERFORMANCE BENCHMARKING (Baseline vs Optimized)", 0, 1, 'L', True)
    pdf.ln(10)
    
    # Comparison Table
    pdf.set_font('helvetica', 'B', 10)
    pdf.set_fill_color(20, 30, 48)
    pdf.set_text_color(255, 255, 255)
    
    lbl_w = 50
    val_w = 45
    pdf.cell(lbl_w, 10, " KPI / Metric", 1, 0, 'L', True)
    pdf.cell(val_w, 10, " Baseline (A*)", 1, 0, 'C', True)
    pdf.cell(val_w, 10, " Optimized Route", 1, 0, 'C', True)
    pdf.cell(val_w, 10, " Variance / Delta", 1, 1, 'C', True)
    
    pdf.set_font('helvetica', '', 10)
    pdf.set_text_color(0, 0, 0)
    
    astar = metrics.get('astar', {})
    optimized = metrics.get('optimized', {})
    
    comparison_data = [
        ("Distance (km)", astar.get('distance_km', 0), optimized.get('distance_km', 0), "km"),
        ("Duration (Hrs)", astar.get('total_hours', 0), optimized.get('total_hours', 0), "h"),
        ("Fuel Consumption (MT)", astar.get('fuel_tonnes', 0), optimized.get('fuel_tonnes', 0), "MT"),
        ("CO2 Emissions (MT)", astar.get('co2_tonnes', 0), optimized.get('co2_tonnes', 0), "MT"),
        ("Avg Safety Risk (%)", astar.get('risk_score', 0), optimized.get('risk_score', 0), "%"),
    ]
    
    for i, (label, v_astar, v_opt, unit) in enumerate(comparison_data):
        fill = (i % 2 == 0)
        pdf.set_fill_color(245, 247, 250)
        pdf.cell(lbl_w, 10, f" {label}", 1, 0, 'L', fill)
        pdf.cell(val_w, 10, f"{v_astar:.1f} {unit}", 1, 0, 'C', fill)
        pdf.cell(val_w, 10, f"{v_opt:.1f} {unit}", 1, 0, 'C', fill)
        
        delta = v_opt - v_astar
        # For risk/fuel/dist/time, negative is usually good (saved)
        # Exception: if it's duration and user wants faster, negative is good.
        is_savings = (delta <= 0)
        if label == "Avg Safety Risk (%)": # Safety risk delta also lower is better
            is_savings = (delta <= 0)
            
        pdf.set_font('helvetica', 'B', 10)
        if delta == 0:
            pdf.set_text_color(100, 100, 100)
            delta_txt = "Optimal"
        else:
            pdf.set_text_color(0, 100, 0) if is_savings else pdf.set_text_color(180, 0, 0)
            prefix = "" if delta < 0 else "+"
            delta_txt = f"{prefix}{delta:.1f} {unit}"
            
        pdf.cell(val_w, 10, delta_txt, 1, 1, 'C', fill)
        pdf.set_font('helvetica', '', 10)
        pdf.set_text_color(0, 0, 0)

    # --- ROUTE VISUALIZATION SECTION ---
    pdf.ln(20)
    pdf.set_font('helvetica', 'B', 14)
    pdf.cell(0, 10, "VOYAGE ROUTE VISUALIZATION", 0, 1, 'L')
    pdf.set_font('helvetica', '', 9)
    pdf.set_text_color(100, 100, 100)
    
    # Placeholder for Map Image
    # Check if map image exists
    map_path = os.path.join(os.path.dirname(__file__), 'route_visualization.png')
    if os.path.exists(map_path):
        pdf.image(map_path, x=10, y=pdf.get_y() + 5, w=190)
    else:
        pdf.set_fill_color(240, 240, 240)
        pdf.rect(10, pdf.get_y() + 5, 190, 100, 'F')
        pdf.set_xy(10, pdf.get_y() + 45)
        pdf.cell(190, 10, "[ Interactive Route Map Screenshot ]", 0, 1, 'C')
        pdf.cell(190, 10, "(Generate and save 'route_visualization.png' to embed)", 0, 1, 'C')

    # --- PAGE 3: AI BRIEFING & SIGNING ---
    pdf.add_page()
    pdf.set_font('helvetica', 'B', 16)
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(0, 12, "5. CAPTAIN'S STRATEGIC BRIEFING (AI)", 0, 1, 'C', True)
    pdf.ln(5)
    pdf.set_font('times', '', 11)
    pdf.multi_cell(0, 7, ai_plan)
    
    # SIGNING BLOCKS
    pdf.ln(20)
    pdf.set_font('helvetica', 'B', 10)
    y_start = pdf.get_y()
    
    # Left Block
    pdf.line(10, y_start + 15, 80, y_start + 15)
    pdf.set_xy(10, y_start + 16)
    pdf.cell(70, 5, "MASTER, M/V OCEAN VOYAGER", 0, 1, 'L')
    pdf.set_font('helvetica', '', 8)
    pdf.cell(70, 4, "Date: ____________________", 0, 1, 'L')
    
    # Right Block
    pdf.set_font('helvetica', 'B', 10)
    pdf.line(130, y_start + 15, 200, y_start + 15)
    pdf.set_xy(130, y_start + 16)
    pdf.cell(70, 5, "CHIEF ENGINEER", 0, 1, 'L')
    pdf.set_font('helvetica', '', 8)
    pdf.cell(70, 4, "Date: ____________________", 0, 1, 'L')
    
    pdf.output(output_path)
    return output_path

def analyze_voyage_with_llm(excel_path, metrics):
    try:
        df = pd.read_excel(excel_path)
        
        # Source comparison from metrics
        astar = metrics.get('astar', {})
        opt = metrics.get('optimized', {})
        
        summary_stats = {
            'wave_height': {'max': df['Wave Height (m)'].max(), 'avg': df['Wave Height (m)'].mean()},
            'wind_speed': {'max': df['Wind Speed (km/h)'].max(), 'avg': df['Wind Speed (km/h)'].mean()},
            'current': {'max': df['Current Velocity (m/s)'].max()},
            'severity': {'max': df['Severity Score (0-100)'].max(), 'avg': df['Severity Score (0-100)'].mean()},
            'visibility': {'min': df['Visibility (m)'].min()},
            'points': len(df),
            'astar': astar,
            'optimized': opt
        }
        
        # Pull request_id from env
        request_id = os.getenv('VOYAGE_REQUEST_ID', 'unknown-req')

        # Build condensed weather digest (top 5 danger zones + stats)
        top5_danger = df.nlargest(5, 'Severity Score (0-100)')[
            ['Latitude', 'Longitude', 'Wave Height (m)', 'Wind Speed (km/h)', 'Visibility (m)', 'Severity Score (0-100)']
        ]
        danger_rows = ""
        for _, row in top5_danger.iterrows():
            danger_rows += f"  ({row['Latitude']:.2f}°, {row['Longitude']:.2f}°) Wave={row['Wave Height (m)']:.1f}m Wind={row['Wind Speed (km/h)']:.0f}km/h Vis={row['Visibility (m)']:.0f}m Severity={row['Severity Score (0-100)']:.1f}\n"
        
        # Risk distribution
        low_risk = len(df[df['Severity Score (0-100)'] <= 30])
        mod_risk = len(df[(df['Severity Score (0-100)'] > 30) & (df['Severity Score (0-100)'] <= 60)])
        high_risk = len(df[df['Severity Score (0-100)'] > 60])
        total_pts = len(df)

        prompt = f"""
        Act as a Senior Master Mariner and Weather Routing Specialist. 
        Analyze the following voyage data, comparing our Optimized Route to the Baseline (A*) Route:
        
        PERFORMANCE COMPARISON:
        - Distance: {astar.get('distance_km', 0):.1f}km (Baseline) vs {opt.get('distance_km', 0):.1f}km (Optimized)
        - Duration: {astar.get('total_hours', 0):.1f}h (Baseline) vs {opt.get('total_hours', 0):.1f}h (Optimized)
        - Fuel Burn: {astar.get('fuel_tonnes', 0):.1f}MT (Baseline) vs {opt.get('fuel_tonnes', 0):.1f}MT (Optimized)
        - Safety Risk: {astar.get('risk_score', 0):.1f}% (Baseline) vs {opt.get('risk_score', 0):.1f}% (Optimized)

        ENVIRONMENTAL STATISTICS ({total_pts} waypoints analyzed):
        - Wave Height: Min={df['Wave Height (m)'].min():.2f}m | Avg={summary_stats['wave_height']['avg']:.2f}m | Max={summary_stats['wave_height']['max']:.2f}m
        - Wind Speed:  Min={df['Wind Speed (km/h)'].min():.1f} | Avg={summary_stats['wind_speed']['avg']:.1f} | Max={summary_stats['wind_speed']['max']:.1f} km/h
        - Visibility:  Min={summary_stats['visibility']['min']:.0f}m | Avg={df['Visibility (m)'].mean():.0f}m | Max={df['Visibility (m)'].max():.0f}m
        - Current:     Max={summary_stats['current']['max']:.2f} m/s
        - Severity:    Avg={summary_stats['severity']['avg']:.1f} | Max={summary_stats['severity']['max']:.1f} / 100

        RISK DISTRIBUTION:
        - Low Risk (0-30):     {low_risk}/{total_pts} waypoints ({100*low_risk/total_pts:.0f}%)
        - Moderate (30-60):    {mod_risk}/{total_pts} waypoints ({100*mod_risk/total_pts:.0f}%)
        - High/Gale (60-100):  {high_risk}/{total_pts} waypoints ({100*high_risk/total_pts:.0f}%)

        TOP 5 HIGHEST-RISK WAYPOINTS:
{danger_rows}
        TASK:
        Provide a detailed 'Strategic Voyage Evaluation' addressed to the Master. 
        Explain WHY the Optimized route was selected over the Baseline (e.g., fuel savings, safety detours, or time optimization).
        Structure your response in 5-6 comprehensive paragraphs covering:
        1. Comparative Assessment: Justification of the optimized path vs the baseline.
        2. Seakeeping Strategy: Advice on peak sea states ({summary_stats['wave_height']['max']:.2f}m peak).
        3. Engine Room Optimization: Strategy for hull stress management vs fuel efficiency.
        4. Tactical Risk: Interpretation of the {opt.get('risk_score', 0):.1f}% average risk vs the baseline, referencing the risk distribution.
        5. Navigation & Bridge Orders: Specific advice on the {high_risk} high-risk waypoints and visibility management.
        
        Tone: Professional, authoritative, maritime-standard. Use "Master" to address the reader.
        """

        openai_api_key = os.getenv('OPENAI_API_KEY')
        if not openai_api_key or openai_api_key == 'your_openai_api_key_here':
            return "NOTICE TO MASTER: AI Strategic Intelligence is currently in DEMO MODE (OpenAI API Key missing).\n\n" + \
                   "SUMMARY ASSESSMENT:\n" + \
                   f"The proposed route encounters peak wave heights of {summary_stats['wave_height']['max']:.2f}m, " + \
                   "which is within standard operating parameters but requires diligent watchkeeping. " + \
                   f"The peak severity score of {summary_stats['severity']['max']:.1f} indicates localized areas of moderate dynamic stress. " + \
                   "Master is advised to maintain current sea-margins and monitor hull vibration if proceeding at standard service speed. " + \
                   "Further fuel optimization is possible by leveraging the favorable current windows identified in the passage plan."

        client = OpenAI(api_key=openai_api_key)
        model_name = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
        
        # Retry with backoff for rate limits (429)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"OpenAI Request: model={model_name} client_id={request_id} attempt={attempt+1}")
                response = client.chat.completions.create(
                    model=model_name,
                    messages=[{"role": "user", "content": prompt}]
                )
                
                result_text = response.choices[0].message.content
                if result_text:
                    logger.info(f"OpenAI Trace: client_id={request_id} status=success")
                    return result_text
                else:
                    raise Exception("OpenAI returned empty response")
            except Exception as retry_err:
                err_str = str(retry_err)
                if '429' in err_str and attempt < max_retries - 1:
                    wait = 60 * (attempt + 1)
                    logger.warning(f"OpenAI rate limited (attempt {attempt+1}). Retrying in {wait}s...")
                    _time.sleep(wait)
                else:
                    raise

    except Exception as e:
        logger.error(f"AI Analysis failed. Error: {e}")
        return f"Strategic analysis unavailable due to technical error: {str(e)}"

def run_full_analysis(excel_path, voyage_metadata, output_pdf):
    logger.info("Starting Comprehensive AI Voyage Analysis...")
    import time
    time.sleep(1.5) 
    
    # Inject request_id into env so the LLM code can find it
    os.environ['VOYAGE_REQUEST_ID'] = voyage_metadata.get('request_id', 'unknown')
    
    ai_plan = analyze_voyage_with_llm(excel_path, voyage_metadata)
    df = pd.read_excel(excel_path)
    
    # Sample waypoints for the table (aim for 20-25 waypoints max for readability)
    step = max(1, len(df) // 22)
    df_sample = df.iloc[::step].copy().reset_index()
    
    analysis_data = {
        'summary': voyage_metadata,
        'dataframe_sample': df_sample,
        'weather': {
            'max_wave': df['Wave Height (m)'].max(),
            'avg_wave': df['Wave Height (m)'].mean(),
            'max_wind': df['Wind Speed (km/h)'].max(),
            'avg_severity': df['Severity Score (0-100)'].mean(),
            'dominant_wave_dir': df['Wave Direction (°)'].mode()[0] if not df['Wave Direction (°)'].empty else 0
        },
        'mode': voyage_metadata.get('label', 'Standard')
    }
    
    generate_voyage_pdf(analysis_data, ai_plan, output_pdf)
    logger.info(f"Multi-page voyage report generated: {output_pdf}")
    return output_pdf
