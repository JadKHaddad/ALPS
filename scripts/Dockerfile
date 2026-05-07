FROM python:3.13-slim

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /scripts

COPY . .

WORKDIR /app

# docker build -t lps-scripts .
# docker run --rm -it lps-scripts python /scripts/latency.py