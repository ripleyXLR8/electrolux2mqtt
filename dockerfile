FROM python:3.13-slim

LABEL org.opencontainers.image.source="https://github.com/ripleyXLR8/electrolux2mqtt"
LABEL org.opencontainers.image.description="Electrolux Group API to MQTT bridge with Home Assistant style discovery"
LABEL org.opencontainers.image.licenses="MIT"

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/ /app/
COPY electrolux2mqtt.conf.template /app/electrolux2mqtt.conf.template

# The configuration file is optional: every setting can also be supplied
# through environment variables, which is what the Unraid template does.
ENTRYPOINT ["python", "/app/electrolux2mqtt.py", "/config/electrolux2mqtt.conf"]
