FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV WM_DB_PATH=/data/wm_central.db
RUN mkdir -p /data

EXPOSE 5000 8080

CMD python wm_central.py ${SOCKET_PORT:-5000} ${KAFKA_BROKER:-broker:29092}