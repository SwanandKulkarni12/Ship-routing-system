# Professional Ship Routing & Voyage Optimization System

A high-performance, physics-aware maritime routing engine that combines global weather data with vessel-specific polar performance models to calculate safe and fuel-efficient ocean routes.

## 🚀 Overview

This system solves the "Least Cost Path" problem for transoceanic voyages by fusing Atmospheric (GFS) and Marine (CMEMS) data into a spatio-temporal navigation graph. Unlike standard routing algorithms, it incorporates:

*   **Unified Physics Model**: Synchronized calculation of fuel surge and time-loss in adverse sea states.
*   **WMO-Standard Safety Scaling**: Non-linear risk assessments based on World Meteorological Organization Sea State scales.
*   **Iterative Corridor Optimization**: A high-efficiency search strategy that focuses computational power on a viable 4D transit window.

## 🛠️ System Architecture

### Backend (Python)
*   **`routing_core.py`**: The central A* / Dijkstra engine.
*   **`vessel_polar.py`**: Physics engine calculating speed loss and fuel surge (Capped at 2.5x for industrial realism).
*   **`gfs_api.py` & `marine_api.py`**: Integration layers for NOAA (Atmospheric) and Copernicus (Marine) data.
*   **`cost_calculation.py`**: Economic weight calculation (Fuel + Hire + Risk Cost).

### Frontend (React + Vite)
*   **Interactive Mapping**: Real-time route visualization with weather-aware color coding.
*   **Metrics Panel**: Comparative analysis of "A* Baseline" vs "Optimized" routes (Distance, ETA, Fuel, CO2).
*   **Progressive UX**: Real-time progress updates via WebSockets.

## 💾 Installation & Setup

### Prerequisites
*   Python 3.10+
*   Node.js 18+
*   Copernicus Marine Credentials (optional, for high-res wave data)

### 1. Backend Setup
```bash
cd Backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt # Or install: networkx numpy scipy websockets requests python-dotenv
```
Create a `.env` file in the `Backend` folder:
```text
# API Credentials
COPERNICUSMARINE_SERVICE_USERNAME=your_user
COPERNICUSMARINE_SERVICE_PASSWORD=your_pass

# Economics
FUEL_PRICE_PER_TONNE=600.0
TIME_CHARTER_RATE_PER_HOUR=2000.0
RISK_COST_PER_UNIT_RISK=10000.0
```
Run the server:
```bash
python3 main.py
```

### 2. Frontend Setup
```bash
cd Frontend
npm install
npm run dev
```

## ⚓ Key Features & Logic

### Unified Performance Model
The system prevents "infinite cost" errors by capping both fuel and time surges at 2.5x the reference rate. This ensures the AI makes realistic commercial trade-offs:
*   **A 1000km detour** is only accepted if the weather gain outweighs the added distance in dollars.
*   **WMO Severity Scaling** ensures a 4.5m wave is treated as moderate risk, while 8.0m+ is a high-penalty "No-Go" zone.

### Data Fusing
The engine automatically switches between high-res marine grids and global atmospheric models to provide the most accurate environmental envelope for the vessel.

## 📜 License
Internal / Commercial Prototype.
