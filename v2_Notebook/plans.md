
**Version 2: The Forecasting Engine** project plan.


## 📅 Project Phase 1: Data Acquisition & Preprocessing

*Goal: Move from a 30-day "snapshot" to a 3-year "climate-aware" dataset.*

1. **Historical Bulk Ingestion:**
* Use the `fmiopendata` library to pull **3 years** of historical data for Oulu (and other target cities).
* **Reason:** This captures the "inter-annual variability" (e.g., comparing a record-breaking cold winter to a mild one).


2. **Temporal Resampling:**
* Downsample raw 10-minute/hourly data into **3-hourly intervals**.
* **Reason:** It reduces database noise and mirrors professional meteorological standards (SYNOP), providing enough granularity to see "morning vs. afternoon" trends without bloating the DB.


3. **Data Imputation (Arctic Logic):**
* Implement "Forward Fill" for short gaps (< 3h).
* Use **Spatial Averaging** (merging data from nearby stations like Oulu Airport + Oulu Pyykösjärvi) to fill longer gaps caused by sensor freezing.



## 🛠️ Project Phase 2: Advanced Feature Engineering

*Goal: Inject physical laws of Finnish meteorology into the model.*

1. **Meteorological Physics Features:**
* **Pressure Tendency:** Calculate $\Delta$ Pressure over 3h, 6h, and 12h. (This is the #1 predictor of approaching "Westerly Disturbances").
* **Dew Point Depression:** ($T - T_{dew}$). If this approaches 0, the model should "expect" fog or precipitation.


2. **Celestial & Cyclical Features:**
* **Solar Elevation Angle:** Use the station’s Lat/Lon to calculate the sun's height. This helps the model distinguish between 12:00 PM in June vs. 12:00 PM in December.
* **Time Encoding:** Convert "Hour of Day" and "Day of Year" into **Sine/Cosine** coordinates so the model understands that Dec 31st is adjacent to Jan 1st.


3. **Geospatial Context:**
* Add a static feature for **Distance to Coast**. (Vital for Oulu, as the sea acts as a heat sink in winter and a cooler in summer).




## 🧠 Project Phase 3: Model Architecture & Training

*Goal: Transition from "Point-in-Time" to "Trend Prediction."*

1. **Multi-Target Output:**
* Configure your XGBoost or LightGBM model to predict a **vector** of results: `[T+24h, T+48h, T+72h]`.
* **Alternative:** Train three separate model "heads" optimized for each specific time horizon.


2. **Horizon-Specific Validation:**
* Use **Time-Series Cross-Validation** (Rolling Window). Never use standard "Random Split" for weather data, or you will "leak" future information into the past.


3. **Uncertainty Quantification:**
* Implement **Quantile Regression** to provide a "Confidence Interval" (e.g., "7°C $\pm$ 2°C").



## ☁️ Project Phase 4: Infrastructure & UI Evolution

*Goal: Scale the Azure/Docker stack to handle the new forecast.*

1. **Database Strategy:**
* **Collection A (Archive):** 3 years of 3-hourly data (Static).
* **Collection B (Live):** Last 7 days of hourly data (TTL Index enabled for auto-cleanup).


2. **The "Worker" Logic Update:**
* The GitHub Action now fetches the "Last 24 hours" to create the "Lag Features" needed for the next 72-hour forecast.


3. **UI/UX (The 3-Day Trend):**
* Replace the single "Current Temp" widget with a **Sparkline Chart** showing the predicted trend for the next 3 days.
* Add a "Naive Disclaimer" badge that links to your research documentation.


## ✅ Success Metrics for V2

* **MAE (Mean Absolute Error):** Aim for $< 1.5$°C for the 24h forecast.
* **Trend Accuracy:** Does the model correctly predict the *direction* (Rising/Falling) of the temperature for 72h?
* **System Latency:** Ensure the GitHub Action + Azure trigger completes in $< 3$ minutes.
