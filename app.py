import os
import logging
from flask import Flask, render_template, jsonify
from pymongo import MongoClient
import pytz
from flask_talisman import Talisman


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev_key_only_change_in_azure')
talisman = Talisman(app, content_security_policy=None, force_https=True)

# Secure Cookie Settings
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# mongodb config
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    logging.error("MONGO_URI not found in environment variables!")

client = MongoClient(MONGO_URI)
db = client.weather_db

# helper to convert time
def format_time(utc_dt):
    try:
        helsinki_tz = pytz.timezone('Europe/Helsinki')
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(helsinki_tz)
        return local_dt.strftime('%Y-%m-%d %H:%M')
    except Exception as e:
        logging.error(f"Time formatting error: {e}")
        return "Time Unavailable"


@app.route('/')
def index():
    try:
        # Get the latest forecast for each unique city
        cities = db.forecasts.distinct("city")
        latest_forecasts = []

        for city in cities:
            forecast = db.forecasts.find_one(
                {"city": city},
                sort=[("timestamp", -1)]
            )
            if forecast:
                # Format the timestamp for the UI
                forecast['display_time'] = format_time(forecast['timestamp'])
                latest_forecasts.append(forecast)

        return render_template('index.html', forecasts=latest_forecasts)
    except Exception as e:
        logging.error(f"Error fetching forecasts: {e}")
        return render_template('index.html', forecasts=[], error="Could not retrieve data.")

# Health check for Azure
@app.route('/healthz', methods=['GET'])
@talisman(force_https=False) # Azure probes often use HTTP
def health_check():
    try:
        # Check MongoDB connection
        if client is None:
            raise Exception('Database client not initialized')
        
        # Ping MongoDB
        result = client.admin.command('ping')
        if result.get('ok') == 1.0:
            return jsonify(status="healthy", database="connected"), 200
        else:
            return jsonify(status="unhealthy", reason="database_ping_failed"), 500
            
    except Exception as e:
        logging.error(f"Health check failed: {e}")
        return jsonify(status="unhealthy", reason=str(e)), 500

if __name__ == '__main__':
    # Azure usually sets the PORT environment variable
    port = int(os.environ.get("PORT", 8000))
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    
    logging.info(f"Starting app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug_mode)