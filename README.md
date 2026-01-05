# Elasticsearch Reranking Pipeline

A pipeline for calculating and updating product trending scores in Elasticsearch based on view metrics from Redshift.

## Overview

This system promotes low-view products (giving them higher trending scores) and demotes high-view products using a logarithmic decay formula:

```
trending_score = max_score - log10(views + 1) * factor
```

Default: `max_score=100`, `factor=30`

## Architecture

```
┌──────────────────┐
│  EventBridge     │──── Daily 2 AM UTC (Scheduled)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐     ┌─────────────┐     ┌───────────────┐
│  Lambda          │────▶│  Redshift   │────▶│ Elasticsearch │
│  es-rerank       │     │  (views)    │     │ (update scores)│
└──────────────────┘     └─────────────┘     └───────────────┘

┌──────────────────┐
│  Docker API      │──── Analytics endpoints
│  (port 5001)     │
└──────────────────┘
```

## Components

| File | Description |
|------|-------------|
| `api.py` | REST API for analytics and manual reranking |
| `lambda_rerank.py` | AWS Lambda function for scheduled reranking |
| `deploy_lambda.py` | Script to deploy Lambda to AWS |
| `rerank_pipeline.py` | Full pipeline CLI tool |
| `rerank.py` | CLI for reranking from CSV file |

## Quick Start

### Docker Hub

```bash
docker pull baljeirani/es-rerank:latest
```

### Run the API

```bash
docker run -d --name es-rerank-api \
  --env-file .env \
  -p 5001:5000 \
  baljeirani/es-rerank:latest python api.py
```

### Run Reranking Manually

```bash
# Via API (dry run)
curl -X POST -H "X-API-Key: your-api-key" "http://localhost:5001/rerank?dry_run=true"

# Via API (apply)
curl -X POST -H "X-API-Key: your-api-key" "http://localhost:5001/rerank?max_score=100&factor=30"
```

## Lambda Deployment

### Prerequisites

- AWS credentials configured in `.env`
- Python 3.11+
- boto3 installed

### Deploy to AWS

```bash
# Install dependencies
pip install boto3 python-dotenv

# Deploy Lambda with EventBridge schedule
python deploy_lambda.py
```

This creates:
- **IAM Role**: `es-rerank-lambda-role` (with Redshift access)
- **S3 Bucket**: `es-rerank-lambda-deployments` (for deployment package)
- **Lambda Function**: `es-rerank`
- **EventBridge Rule**: `es-rerank-daily` (runs at 2 AM UTC)

### Invoke Lambda Manually

```bash
# Dry run
aws lambda invoke --function-name es-rerank \
  --payload '{"dry_run": true}' \
  --region me-south-1 output.json

# Full execution
aws lambda invoke --function-name es-rerank \
  --payload '{"max_score": 100, "factor": 30}' \
  --region me-south-1 output.json
```

### Lambda Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_score` | 100 | Maximum trending score |
| `factor` | 30 | Decay factor (higher = steeper decay) |
| `dry_run` | false | Preview without updating |
| `index` | skus_product_pool_v3 | Elasticsearch index |

### Change Schedule

Edit EventBridge rule in AWS Console or update `deploy_lambda.py`:

```python
# Daily at 2 AM UTC
'cron(0 2 * * ? *)'

# Every 6 hours
'rate(6 hours)'

# Every day at midnight
'cron(0 0 * * ? *)'
```

## Environment Variables

Create a `.env` file:

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
REDSHIFT_CLUSTER_ID=your-cluster-id

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

### Analytics (GET)

| Endpoint | Auth | Description |
|----------|------|-------------|
| `/health` | No | Health check |
| `/stats` | Yes | Overall statistics |
| `/distribution/views` | Yes | Products by view ranges |
| `/distribution/scores` | Yes | Products by score ranges |
| `/top?limit=20` | Yes | Top trending products |
| `/bottom?limit=20` | Yes | Bottom trending products |
| `/summary` | Yes | Complete summary |
| `/sku/<sku_id>` | Yes | Get SKU details |
| `/download` | Yes | Download metrics CSV |
| `/download?format=json` | Yes | Download metrics JSON |

### Actions (POST)

| Endpoint | Auth | Description |
|----------|------|-------------|
| `/rerank` | Yes | Trigger reranking |
| `/rerank?dry_run=true` | Yes | Preview reranking |
| `/rerank?max_score=100&factor=30` | Yes | Custom formula |

### Authentication

Use `X-API-Key` header or `api_key` query parameter:

```bash
curl -H "X-API-Key: your-api-key" http://localhost:5001/stats
```

## Score Distribution

### Factor Comparison

| Views | Factor=25 | Factor=30 |
|-------|-----------|-----------|
| 0 | 100 | 100 |
| 10 | 74 | 69 |
| 100 | 50 | 40 |
| 1,000 | 25 | 10 |
| 10,000 | 0 | 0 |

Higher factor = more aggressive demotion of popular products.

### Example Distribution (factor=30)

| Score Range | Avg Views | Description |
|-------------|-----------|-------------|
| 90-100 | 0-1 | New/unseen products |
| 75-90 | 3 | Low visibility |
| 50-75 | 20 | Moderate views |
| 25-50 | 116 | Popular |
| 10-25 | 464 | Very popular |
| 0-10 | 306 | Most viewed |

## Docker Compose

```bash
# Start API
docker-compose up -d api

# Run reranking pipeline
docker-compose run rerank python rerank_pipeline.py --apply
```

## Postman Collection

Import `elasticsearch-rerank.postman_collection.json` for all API endpoints.

## GitHub

https://github.com/baljeirani2022/elastice-rerank

## Docker Hub

https://hub.docker.com/r/baljeirani/es-rerank

## License

MIT
