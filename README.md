# 🌍 Real-time Air Quality & Weather Monitoring (IoT/Environment)
This project fetches Air Quality Index (AQI), pollutant levels (PM2.5, PM10), and weather data (temperature, humidity, etc.) from the [OpenWeatherMap API](https://openweathermap.org/api), transformed, stores them in **PostgreSQL**, and provides visualization options (**Jupyter Notebook** + **Grafana** dashboards).

---

## CAPSTONE PROJECT FOR GROUP 8

## 🚀 Features
1. **Python Script (Jupyter Notebook)**  
   - Fetches hourly **Air Quality Index (AQI)**, **temperature**, **humidity**, and **pollution levels**.
   - Performs transformations to calculate **AQI categories** and **detect spikes**.

2. **PostgreSQL Database**  
   - Stores sensor readings (time-series) and alerts.  
   - Designed schema for scalability.

3. **Grafana Dashboard**  
   - **AQI Trends** (line chart).  
   - **Location-based Pollution Heatmap** (map visualization).  
   - **Weather vs AQI Correlation** (scatter/overlay charts).
