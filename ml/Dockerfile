FROM python:3.11-slim

WORKDIR /workspace

# Zależności systemowe: kompilator C++ wymagany przez Prophet (cmdstanpy)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ cmake curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8888

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", "--port=8888", \
     "--no-browser", "--allow-root", \
     "--NotebookApp.token=", "--NotebookApp.password="]
