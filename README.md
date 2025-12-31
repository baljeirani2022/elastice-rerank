# Elasticsearch Reranking Pipeline

A Docker-based pipeline for calculating and updating product trending scores in Elasticsearch based on view metrics from Redshift.

## Overview

This system promotes low-view products (giving them higher trending scores) and demotes high-view products using a logarithmic decay formula:

```
trending_score = 100 - log10(views + 1) * 25
```

## Components

- **rerank_pipeline.py** - Full pipeline: Redshift → Calculate scores → Update Elasticsearch
- **rerank.py** - CLI for reranking from existing CSV file
- **api.py** - Secured REST API for analytics and distributions
- **fetch_product_metrics.py** - Fetch product metrics from Redshift

## Quick Start

### Using Docker Hub

```bash
docker pull baljeirani/elastic-rerank:latest
```

### Run the API

```bash
docker run -d --env-file .env -p 5000:5000 baljeirani/elastic-rerank:latest python api.py
```

### Run the Reranking Pipeline

```bash
# Dry run (preview only)
docker run --env-file .env baljeirani/elastic-rerank:latest python rerank_pipeline.py --dry-run

# Apply changes
docker run --env-file .env baljeirani/elastic-rerank:latest python rerank_pipeline.py --apply
```

## Environment Variables

Create a `.env` file with:

```env
# AWS Credentials
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=me-south-1

# Redshift Connection
REDSHIFT_HOST=your-redshift-host
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=your-database
REDSHIFT_USER=your-user

# Elasticsearch
ELASTICSEARCH_HOST=https://your-es-host
ELASTICSEARCH_PORT=9243
ELASTICSEARCH_USERNAME=elastic
ELASTICSEARCH_PASSWORD=your-password

# API Configuration
API_KEY=your-api-key
API_PORT=5000
ES_INDEX=skus_product_pool_v3
```

## API Endpoints

| Endpoint | Auth | Description |
|----------|------|-------------|
| GET /health | No | Health check |
| GET /stats | Yes | Overall statistics |
| GET /distribution/views | Yes | Products by view ranges |
| GET /distribution/scores | Yes | Products by score ranges |
| GET /top?limit=20 | Yes | Top trending products |
| GET /bottom?limit=20 | Yes | Bottom trending products |
| GET /summary | Yes | Complete summary |

### Authentication

Use `X-API-Key` header or `api_key` query parameter:

```bash
curl -H "X-API-Key: your-api-key" http://localhost:5000/stats
```

## Docker Compose

```bash
# Start API
docker-compose up -d api

# Run reranking pipeline
docker-compose run rerank python rerank_pipeline.py --apply
```

## Score Distribution

The logarithmic formula creates this distribution:

| Views | Trending Score |
|-------|----------------|
| 0 | 100 |
| 10 | 74 |
| 100 | 50 |
| 1,000 | 25 |
| 10,000 | 0 |

## License

MIT
