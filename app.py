import os
from flask import Flask, render_template
from pymongo import MongoClient
from datetime import datetime
import pytz # Critical for UTC to Finland conversion

app = Flask(__name__)
print(f"Flask is looking for templates in: {app.template_folder}")
# --- CONFIGURATION ---
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client.weather_db

def format_time(utc_dt):
    """Converts UTC datetime from MongoDB to Finnish Local Time."""
    helsinki_tz = pytz.timezone('Europe/Helsinki')
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(helsinki_tz)
    return local_dt.strftime('%Y-%m-%d %H:%M')

@app.route('/')
def index():
    # Get the latest forecast for each unique city
    cities = db.forecasts.distinct("city")
    latest_forecasts = []

    for city in cities:
        forecast = db.forecasts.find_one(
            {"city": city},
            sort=[("timestamp", -1)] # Get the newest one
        )
        if forecast:
            # Format the timestamp for the UI
            forecast['display_time'] = format_time(forecast['timestamp'])
            latest_forecasts.append(forecast)

    return render_template('index.html', forecasts=latest_forecasts)

if __name__ == '__main__':
    # Azure uses port 8000 or the PORT env var by default
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(debug=debug_mode)