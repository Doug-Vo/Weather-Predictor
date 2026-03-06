FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

WORKDIR /app

# Install dependencies first (layer caching — only rebuilds if requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files only
COPY app.py .
COPY templates/ templates/

EXPOSE 8000

CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:$PORT --timeout 60 --workers 2 app:app"]