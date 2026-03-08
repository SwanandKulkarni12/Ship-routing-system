import os
import json
import logging
import pandas as pd
from fpdf import FPDF
from openai import OpenAI
from datetime import datetime

logger = logging.getLogger(__name__)

class VoyageReport(FPDF):
    def header(self):
        self.set_fill_color(20, 30, 48)
        self.rect(0, 0, 210, 40, 'F')
        self.set_font('helvetica', 'B', 24)
        self.set_text_color(255, 255, 255)
        self.cell(0, 20, 'VOYAGE STRATEGIC ANALYSIS', 0, 1, 'C')
        self.set_font('helvetica', 'I', 10)
        self.cell(0, 5, f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 0, 1, 'C')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('helvetica', 'I', 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f'Page {self.page_no()} | Confidential Maritime Routing Report', 0, 0, 'C')

def generate_voyage_pdf(analysis_data, ai_plan, output_path):
    pdf = VoyageReport()
    pdf.add_page()
    
    # 1. Summary Section
    pdf.set_font('helvetica', 'B', 16)
    pdf.set_text_color(20, 30, 48)
    pdf.cell(0, 10, '1. VOYAGE SUMMARY', 0, 1, 'L')
    pdf.set_font('helvetica', '', 11)
    
    data = analysis_data['summary']
    pdf.cell(90, 8, f"Total Distance: {data.get('total_dist_nm', 0):.1f} NM", 1)
    pdf.cell(90, 8, f"Estimated Duration: {data.get('total_hours', 0):.1f} Hours", 1, 1)
    pdf.cell(90, 8, f"Total Fuel (HFO): {data.get('fuel_tonnes', 0):.1f} MT", 1)
    pdf.cell(90, 8, f"CO2 Emission: {data.get('co2_tonnes', 0):.1f} MT", 1, 1)
    pdf.ln(5)

    # 2. Weather Severity
    pdf.set_font('helvetica', 'B', 16)
    pdf.cell(0, 10, '2. WEATHER EXPOSURE ANALYSIS', 0, 1, 'L')
    pdf.set_font('helvetica', '', 11)
    
    w = analysis_data['weather']
    pdf.cell(90, 8, f"Max Wave Height: {w.get('max_wave', 0):.2f}m", 1)
    pdf.cell(90, 8, f"Avg Wave Height: {w.get('avg_wave', 0):.2f}m", 1, 1)
    pdf.cell(90, 8, f"Max Wind Speed: {w.get('max_wind', 0):.1f} km/h", 1)
    pdf.cell(90, 8, f"Significant Wave Direction: {w.get('dominant_wave_dir', 0):.0f} deg", 1, 1)
    pdf.ln(5)

    # 3. AI Strategic Plan
    pdf.set_font('helvetica', 'B', 16)
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(0, 10, "3. CAPTAIN'S STRATEGIC BRIEFING (AI)", 0, 1, 'L', True)
    pdf.ln(2)
    pdf.set_font('helvetica', '', 10)
    pdf.multi_cell(0, 6, ai_plan)
    
    pdf.output(output_path)
    return output_path

def analyze_voyage_with_llm(excel_path):
    try:
        df = pd.read_excel(excel_path)
        
        # Aggregate data for context
        summary_stats = {
            'wave_height': {'max': df['Wave Height (m)'].max(), 'avg': df['Wave Height (m)'].mean()},
            'wind_speed': {'max': df['Wind Speed (km/h)'].max(), 'avg': df['Wind Speed (km/h)'].mean()},
            'current': {'max': df['Current Velocity (m/s)'].max()},
            'severity': {'max': df['Severity Score (0-100)'].max(), 'avg': df['Severity Score (0-100)'].mean()},
            'visibility': {'min': df['Visibility (m)'].min()},
            'points': len(df)
        }
        
        prompt = f"""
        Act as a Master Mariner and Weather Router.
        Analyze this voyage data:
        Max Wave: {summary_stats['wave_height']['max']:.2f}m
        Avg Wave: {summary_stats['wave_height']['avg']:.2f}m
        Max Wind: {summary_stats['wind_speed']['max']:.1f} km/h
        Max Current: {summary_stats['current']['max']:.2f} m/s
        Min Visibility: {summary_stats['visibility']['min']:.0f} m
        Peak Severity Score (0-100): {summary_stats['severity']['max']:.1f}
        Number of Waypoints: {summary_stats['points']}

        Provide a concise 'Strategic Voyage Plan' in 4-5 paragraphs. 
        Focus on:
        1. Safety margins given the wave heights.
        2. Speed optimization strategy (Engine load).
        3. Potential risk areas.
        4. Weather avoidance advice.
        """

        client = OpenAI(
            api_key=os.getenv('OPENAI_API_KEY', 'dummy_key'),
            base_url=os.getenv('OPENAI_API_BASE', 'https://api.openai.com/v1')
        )
        
        # If no real key, return a default professional fallback
        if os.getenv('OPENAI_API_KEY') == 'your_openai_api_key_here' or not os.getenv('OPENAI_API_KEY'):
            return "Note: AI Analysis is in demo mode (Please provide valid OPENAI_API_KEY to enable live GPT-4 strategic planning).\n\n" + \
                   "STRATEGIC RECOMMENDATION:\n" + \
                   f"The voyage encounters a peak wave height of {summary_stats['wave_height']['max']:.2f}m. " + \
                   "A conservative engine load is recommended during peak sea states to minimize hull stress. " + \
                   "Current data suggests favorable windows for speed-up if schedule pressure exists, but sea-margins must be maintained."

        response = client.chat.completions.create(
            model=os.getenv('OPENAI_MODEL', 'gpt-4'),
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"AI Analysis failed. Columns found: {df.columns.tolist() if 'df' in locals() else 'N/A'}. Error: {e}")
        return f"Strategic analysis unavailable due to technical error: {str(e)}"

def run_full_analysis(excel_path, voyage_metadata, output_pdf):
    logger.info("Starting AI Voyage Analysis...")
    # Ensure file is flushed to disk
    import time
    time.sleep(2) 
    ai_plan = analyze_voyage_with_llm(excel_path)
    
    df = pd.read_excel(excel_path)
    analysis_data = {
        'summary': voyage_metadata,
        'weather': {
            'max_wave': df['Wave Height (m)'].max(),
            'avg_wave': df['Wave Height (m)'].mean(),
            'max_wind': df['Wind Speed (km/h)'].max(),
            'dominant_wave_dir': df['Wave Direction (°)'].mode()[0] if not df['Wave Direction (°)'].empty else 0
        }
    }
    
    generate_voyage_pdf(analysis_data, ai_plan, output_pdf)
    logger.info(f"Voyage report generated: {output_pdf}")
    return output_pdf
