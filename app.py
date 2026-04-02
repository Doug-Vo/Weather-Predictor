import os
import logging
import requests, uuid
from flask import Flask, render_template, request, jsonify
from flask_wtf.csrf import CSRFProtect
from flask_talisman import Talisman
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev_key_only_change_in_azure')

# Security Config
talisman = Talisman(app, content_security_policy=None, force_https=True)

app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# CSRF Protection
csrf = CSRFProtect(app)

# Rate Limiting
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

# Azure Translator Config
AZURE_KEY = os.environ.get("AZURE_TRANSLATOR_KEY")
AZURE_LOCATION = os.environ.get("AZURE_TRANSLATOR_LOCATION")
AZURE_ENDPOINT = "https://api.cognitive.microsofttranslator.com/"

if not AZURE_KEY:
    logging.error("AZURE_TRANSLATOR_KEY not found in environment variables!")
if not AZURE_LOCATION:
    logging.error("AZURE_TRANSLATOR_LOCATION not found in environment variables!")

LANGUAGES = ['en', 'fi', 'vi', 'zh-Hans']


def translate(text, to_lang, from_lang=None):
    try:
        url = f"{AZURE_ENDPOINT}translate"
        params = {"api-version": "3.0", "to": to_lang}
        if from_lang:
            params["from"] = from_lang
        if to_lang == "zh-Hans":
            params["toScript"] = "Latn"  # Pinyin romanization

        headers = {
            "Ocp-Apim-Subscription-Key": AZURE_KEY,
            "Ocp-Apim-Subscription-Region": AZURE_LOCATION,
            "Content-Type": "application/json",
            "X-ClientTraceId": str(uuid.uuid4())
        }
        response = requests.post(url, params=params, headers=headers, json=[{"text": text}])
        response.raise_for_status()
        result = response.json()[0]["translations"][0]

        if to_lang == "zh-Hans" and "transliteration" in result:
            return f"{result['text']}\n{result['transliteration']['text']}"

        return result["text"]

    except Exception as e:
        logging.error(f"Translation error ({from_lang} -> {to_lang}): {e}")
        raise


# Custom Error Handler for Rate Limiting
@app.errorhandler(429)
def ratelimit_handler(e):
    return jsonify({"error": "You are translating too fast! Please wait a moment before trying again."}), 429


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/api/translate', methods=['POST'])
@limiter.limit("40 per minute")
def api_translate():
    try:
        query = request.get_json()
        original_text = query.get("text")
        source_lang = query.get("source_lang")

        if not original_text or not source_lang:
            return jsonify({"error": "Missing Data!"}), 400

        results = {}
        for lang in LANGUAGES:
            if lang != source_lang:
                translated = translate(original_text, from_lang=source_lang, to_lang=lang)
                results[lang] = translated
                logging.info(f"Translated '{original_text}' ({source_lang}) -> '{translated}' ({lang})")

        return jsonify(results)

    except Exception as e:
        logging.error(f"Translation request failed: {e}")
        return jsonify({"error": "Translation service unavailable. Please try again."}), 500

from flask import send_from_directory

@app.route('/image.png')
def serve_image():
    return send_from_directory('.', 'image.png')

@app.route('/healthz', methods=['GET'])
@talisman(force_https=False)
def health_check():
    try:
        if not AZURE_KEY or not AZURE_LOCATION:
            raise Exception("Azure Translator credentials not configured")
        return jsonify(status="healthy", service="azure-translator"), 200
    except Exception as e:
        logging.error(f"Health check failed: {e}")
        return jsonify(status="unhealthy", reason=str(e)), 500


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    logging.info(f"Starting app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug_mode)