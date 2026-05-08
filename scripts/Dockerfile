FROM python:3.13-slim

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /scripts

COPY . .

WORKDIR /app

# docker build -t jadkhaddad/lps-scripts:0.0.1 .
# docker run --rm -it jadkhaddad/lps-scripts:0.0.1 python /scripts/latency.py

# docker push jadkhaddad/lps-scripts:0.0.1