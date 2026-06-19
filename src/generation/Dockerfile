FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc g++ libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ /app/src/

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "generation.main"]
