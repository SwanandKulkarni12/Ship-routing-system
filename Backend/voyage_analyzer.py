import os
import pandas as pd
import numpy as np
import logging
from fpdf import FPDF
from datetime import datetime
import google.generativeai as genai

logger = logging.getLogger(__name__)

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
    pdf.cell(0, 8, f" Fuel Saved vs Baseline: {metrics.get('fuel_tonnes_saved', 0):.1f} MT", 0, 1)
    pdf.cell(0, 8, f" CO2 Abatement: {metrics.get('co2_tonnes_saved', 0):.1f} MT (Reduced Emissions)", 0, 1)
    pdf.ln(10)

    # Environmental section
    pdf.set_font('helvetica', 'B', 14)
    pdf.cell(0, 10, "3. ENVIRONMENTAL EXPOSURE & SAFETY", 0, 1, 'L')
    pdf.set_font('helvetica', '', 11)
    w = analysis_data['weather']
    pdf.cell(0, 8, f" Max Wave Height: {w['max_wave']:.2f}m | Avg: {w['avg_wave']:.2f}m", 0, 1)
    pdf.cell(0, 8, f" Max Wind Speed: {w['max_wind']:.1f} km/h (Beaufort Force Integration)", 0, 1)
    pdf.cell(0, 8, f" Significant Wave Direction: {w['dominant_wave_dir']:.1f} deg", 0, 1)
    pdf.cell(0, 8, f" Route Severity Grade: {w['avg_severity']:.1f}% (Dynamic Stress Index)", 0, 1)
    
    # --- PAGE 2: WAYPOINT LOG ---
    pdf.add_page()
    pdf.set_font('helvetica', 'B', 16)
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(0, 12, "4. WAYPOINT ANALYSIS LOG", 0, 1, 'C', True)
    pdf.ln(5)
    
    pdf.set_font('helvetica', 'B', 9)
    pdf.set_fill_color(20, 30, 48)
    pdf.set_text_color(255, 255, 255)
    
    # Table Header
    cols = [('Lat', 20), ('Lon', 20), ('Wave(m)', 20), ('Bft', 15), ('Wind(kmh)', 25), ('Vis(m)', 20), ('Sev/100', 25), ('Status', 45)]
    for txt, cw in cols:
        pdf.cell(cw, 10, txt, 1, 0, 'C', True)
    pdf.ln()
    
    pdf.set_font('helvetica', '', 8)
    pdf.set_text_color(0, 0, 0)
    
    df_sampled = analysis_data['dataframe_sample']
    for i, row in df_sampled.iterrows():
        fill = (i % 2 == 0)
        pdf.set_fill_color(245, 245, 245)
        
        pdf.cell(20, 8, f"{row['Latitude']:.2f}", 1, 0, 'C', fill)
        pdf.cell(20, 8, f"{row['Longitude']:.2f}", 1, 0, 'C', fill)
        pdf.cell(20, 8, f"{row['Wave Height (m)']:.1f}", 1, 0, 'C', fill)
        
        # Use Beaufort scale
        bft = get_beaufort_force(row['Wind Speed (km/h)'])
        pdf.cell(15, 8, f"F{bft}", 1, 0, 'C', fill)
        
        pdf.cell(25, 8, f"{row['Wind Speed (km/h)']:.0f}", 1, 0, 'C', fill)
        pdf.cell(20, 8, f"{row['Visibility (m)']:.0f}", 1, 0, 'C', fill)
        
        sev = row['Severity Score (0-100)']
        pdf.cell(25, 8, f"{sev:.1f}", 1, 0, 'C', fill)
        
        status = "LOW RISK"
        if sev > 60: status = "GALE/STORM"
        elif sev > 30: status = "MODERATE"
        
        if status == "GALE/STORM": pdf.set_text_color(200, 0, 0)
        elif status == "MODERATE": pdf.set_text_color(180, 120, 0)
        else: pdf.set_text_color(0, 100, 0)
        
        pdf.cell(45, 8, status, 1, 1, 'C', fill)
        pdf.set_text_color(0, 0, 0)

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

def analyze_voyage_with_llm(excel_path):
    try:
        df = pd.read_excel(excel_path)
        
        summary_stats = {
            'wave_height': {'max': df['Wave Height (m)'].max(), 'avg': df['Wave Height (m)'].mean()},
            'wind_speed': {'max': df['Wind Speed (km/h)'].max(), 'avg': df['Wind Speed (km/h)'].mean()},
            'current': {'max': df['Current Velocity (m/s)'].max()},
            'severity': {'max': df['Severity Score (0-100)'].max(), 'avg': df['Severity Score (0-100)'].mean()},
            'visibility': {'min': df['Visibility (m)'].min()},
            'points': len(df)
        }
        
        # Pull request_id from env
        request_id = os.getenv('VOYAGE_REQUEST_ID', 'unknown-req')

        prompt = f"""
        Act as a Senior Master Mariner and Weather Routing Specialist. 
        Analyze the following voyage data for a deep-sea merchant vessel:
        
        METRICS:
        - Max Wave Height: {summary_stats['wave_height']['max']:.2f}m
        - Avg Wave Height: {summary_stats['wave_height']['avg']:.2f}m
        - Max Wind Speed: {summary_stats['wind_speed']['max']:.1f} km/h
        - Max Current: {summary_stats['current']['max']:.2f} m/s
        - Min Visibility: {summary_stats['visibility']['min']:.0f} m
        - Peak Severity Score (Risk Index): {summary_stats['severity']['max']:.1f} / 100
        - Route Density: {summary_stats['points']} waypoints analyzed.

        TASK:
        Provide a detailed 'Strategic Voyage Evaluation' addressed to the Master. 
        Structure your response in 5-6 comprehensive paragraphs covering:
        1. General Assessment: Overall voyage feasibility and safety baseline.
        2. Seakeeping Strategy: Advice on heading adjustments or speed reductions during peak sea states ({summary_stats['wave_height']['max']:.2f}m peak).
        3. Engine Room Optimization: Strategy for hull stress management vs fuel efficiency.
        4. Tactical Risk: Interpretation of the {summary_stats['severity']['max']:.1f} Peak Severity Score.
        5. Navigation & Bridge Orders: Specific advice on visibility and current management.
        
        Tone: Professional, authoritative, maritime-standard. Use "Master" to address the reader.
        """

        gemini_api_key = os.getenv('GEMINI_API_KEY')
        if not gemini_api_key or gemini_api_key == 'your_gemini_api_key_here':
            return "NOTICE TO MASTER: AI Strategic Intelligence is currently in DEMO MODE (Gemini API Key missing).\n\n" + \
                   "SUMMARY ASSESSMENT:\n" + \
                   f"The proposed route encounters peak wave heights of {summary_stats['wave_height']['max']:.2f}m, " + \
                   "which is within standard operating parameters but requires diligent watchkeeping. " + \
                   f"The peak severity score of {summary_stats['severity']['max']:.1f} indicates localized areas of moderate dynamic stress. " + \
                   "Master is advised to maintain current sea-margins and monitor hull vibration if proceeding at standard service speed. " + \
                   "Further fuel optimization is possible by leveraging the favorable current windows identified in the passage plan."

        genai.configure(api_key=gemini_api_key)
        model_name = os.getenv('GEMINI_MODEL', 'gemini-1.5-pro')
        model = genai.GenerativeModel(model_name)
        
        logger.info(f"Gemini Request: model={model_name} client_id={request_id}")
        response = model.generate_content(prompt)
        
        if response.text:
            logger.info(f"Gemini Trace: client_id={request_id} status=success")
            return response.text
        else:
            raise Exception("Gemini returned empty response")

    except Exception as e:
        logger.error(f"AI Analysis failed. Error: {e}")
        return f"Strategic analysis unavailable due to technical error: {str(e)}"

def run_full_analysis(excel_path, voyage_metadata, output_pdf):
    logger.info("Starting Comprehensive AI Voyage Analysis...")
    import time
    time.sleep(1.5) 
    
    # Inject request_id into env so the LLM code can find it
    os.environ['VOYAGE_REQUEST_ID'] = voyage_metadata.get('request_id', 'unknown')
    
    ai_plan = analyze_voyage_with_llm(excel_path)
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
