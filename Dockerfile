FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application scripts
COPY rerank.py .
COPY rerank_pipeline.py .
COPY fetch_product_metrics.py .
COPY api.py .

# Create data directory for CSV files
RUN mkdir -p /data

# Expose API port
EXPOSE 5000

# Default command shows help
CMD ["python", "rerank_pipeline.py", "--help"]
