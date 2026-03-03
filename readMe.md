# Finland Weather Predictor

### **Purpose**

A machine learning pipeline that predicts Finnish weather transitions using real-time data from the **[Finnish Meteorological Institute (FMI)](https://en.ilmatieteenlaitos.fi/)**.

> **Note on Model Maturity:** This project is currently in its **first iteration**. The underlying ML model is intentionally simple and "naive," serving as a proof-of-concept for the end-to-end data pipeline before moving toward more complex architectures.

### **Features**

1. **Automated Collection:** Hourly ingestion of airport and city-center station data via WFS.
2. **Robust Preprocessing:** Spatial merging of fragmented sensors to ensure data continuity. 
[![Hourly Weather Inference](https://github.com/Doug-Vo/Weather-Predictor/actions/workflows/worker.yml/badge.svg)](https://github.com/Doug-Vo/Weather-Predictor/actions/workflows/worker.yml)
3. **Cloud-Native Deployment:** Fully containerized architecture with automated CI/CD.

### **Deployment Pipeline**

* **Automation (GitHub Actions):** Runs hourly to fetch FMI weather data, perform ML inference, and sync results to MongoDB.
* **Containerization (Docker):** Packages the app into a consistent environment, ensuring identical performance from local to cloud.
* **Cloud Hosting (Azure):** Hosted on **Azure App Service**, pulling the latest image from **Docker Hub** to serve a secure, real-time UI.


---

*To be updated*
