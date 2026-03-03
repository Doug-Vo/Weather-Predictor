

# Finland Weather Predictor

### **Purpose**

The goal of this project is to build a  machine learning pipeline that predicts Finnish weather transitions (specifically temperature and precipitation) using real-time data from the [**Finnish Meteorological Institute (FMI)**](https://en.ilmatieteenlaitos.fi/).



### **Features**


1. **Weather Data Collection:** Using specific airport and city-center station IDs.
2. **Robust Preprocessing** Using multiple observation centers to fill in the data for a specific city
   
3. **Scalable Deployment:** Hosted on **Azure** along with a Database in **MongoDB**



### **What I Have Done (The Research Phase)**

* **Data Collection:** 30 days of multi-city FMI data, implementing 
* **Spatial Merging** to consolidate fragmented station sensors and **meteorological imputation** using impute sensor gaps during Arctic conditions.
* **Geospatial & Time-Series Modeling:** integrating **latitude/longitude features** 


### **The Plan (Current & Next Steps)**

1. **Feature Engineering:**
   * Implement **Temporal Lags** (T-1, T-2h) to capture weather momentum.
   * Create **Cyclical Features** (Sine/Cosine) for "Hour of Day" to represent the 24-hour cycle mathematically.


2. **Model Training:** Train an **XGBoost** or **Random Forest** regressor to predict the temperature for the upcoming hour.
   
3. **Infrastructure & Deployment:**
   * **Storage:** Set up **Azure Cosmos DB (MongoDB API)** to cache FMI data and avoid redundant API calls.
   * **Backend:** Deploy the predictor as an **Azure Function** (Python) for serverless scalability.
   * **Frontend:** Create a clean dashboard hosted on **Azure Static Web Apps** for real-time visualization.



---

### **Technical Stack**

* **Language:** Python (Pandas, Matplotlib, Scikit-Learn)
* **API:** FMI Open Data (WFS)
* **Database:** MongoDB / Azure Cosmos DB
* **Cloud:** Microsoft Azure


### **How to Use**

*To be updated*


![Status](https://github.com/[Your-Username]/[Your-Repo-Name]/actions/workflows/worker.yml/badge.svg)