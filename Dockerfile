# Gunakan image Python
FROM python:3.11-slim

WORKDIR /app

# Copy pyproject.toml (dependency FastAPI & Uvicorn)
COPY pyproject.toml .

# Install dependencies langsung via pip
RUN pip install --no-cache-dir .

# Copy source code
COPY src/ ./src/

# Set environment variable untuk DB
ENV DEDUP_DB=/data/dedup.db

# Buat folder data
RUN mkdir -p /data

# Expose port FastAPI
EXPOSE 8000

# Jalankan FastAPI
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
