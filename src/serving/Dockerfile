FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc g++ libpq-dev curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "serving.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
