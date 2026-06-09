FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY stream_processor.py .

CMD ["python", "-u", "stream_processor.py"]
