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

app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    logging.error("MONGO_URI not found in environment variables!")

client = MongoClient(MONGO_URI)
db = client.weather_db

CODE_LABELS = {0: "Clear", 1: "Rain", 2: "Snow"}
CODE_ICONS  = {0: "bi-sun",  1: "bi-cloud-rain", 2: "bi-snow"}

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
        cities = db.forecastV2.distinct("city")
        latest_forecasts = []

        for city in cities:
            forecast = db.forecastV2.find_one(
                {"city": city},
                sort=[("timestamp", -1)]
            )
            if forecast:
                forecast['display_time'] = format_time(forecast['timestamp'])

                # Attach code labels for template
                forecast['current']['code_label'] = CODE_LABELS.get(forecast['current'].get('code', 0), "Clear")
                forecast['current']['code_icon']  = CODE_ICONS.get(forecast['current'].get('code', 0), "bi-sun")

                for h in ['3h', '6h', '12h']:
                    code = forecast.get('code_forecast', {}).get(h, 0)
                    forecast['code_forecast'][f'{h}_label'] = CODE_LABELS.get(code, "Clear")
                    forecast['code_forecast'][f'{h}_icon']  = CODE_ICONS.get(code, "bi-sun")

                for h in ['6h', '12h', '24h']:
                    fmi = forecast.get('fmi_forecast', {}).get(h, {})
                    if fmi:
                        code = fmi.get('code', 0)
                        forecast['fmi_forecast'][h]['code_label'] = CODE_LABELS.get(code, "Clear")
                        forecast['fmi_forecast'][h]['code_icon']  = CODE_ICONS.get(code, "bi-sun")

                latest_forecasts.append(forecast)

        # Sort cities alphabetically
        latest_forecasts.sort(key=lambda x: x['city'])

        return render_template('index.html', forecasts=latest_forecasts)
    except Exception as e:
        logging.error(f"Error fetching forecasts: {e}")
        return render_template('index.html', forecasts=[], error="Could not retrieve data.")


@app.route('/healthz', methods=['GET'])
@talisman(force_https=False)
def health_check():
    try:
        if client is None:
            raise Exception('Database client not initialized')
        result = client.admin.command('ping')
        if result.get('ok') == 1.0:
            return jsonify(status="healthy", database="connected"), 200
        else:
            return jsonify(status="unhealthy", reason="database_ping_failed"), 500
    except Exception as e:
        logging.error(f"Health check failed: {e}")
        return jsonify(status="unhealthy", reason=str(e)), 500


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    logging.info(f"Starting app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug_mode)