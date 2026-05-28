FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade certifi -r requirements.txt

COPY historical_fetch.py .

CMD ["python", "-u", "historical_fetch.py"]