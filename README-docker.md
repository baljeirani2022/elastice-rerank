# Elasticsearch Product Reranking - Docker

Rerank products in Elasticsearch based on view counts. Promotes low-view products (high score) and demotes high-view products (low score).

## Formula

```
trending_score = 100 - log10(views + 1) * 25
```

| Views | Score |
|-------|-------|
| 0 | 100 |
| 10 | 74 |
| 100 | 50 |
| 1,000 | 25 |
| 10,000+ | 0 |

## Quick Start (EC2)

### 1. Install Docker on EC2

```bash
sudo yum update -y
sudo yum install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
# Log out and back in for group changes
```

### 2. Copy Files to EC2

```bash
scp -r elastic/ ec2-user@<EC2_IP>:~/rerank/
```

### 3. Create .env File

```bash
cd ~/rerank
cat > .env << EOF
ELASTICSEARCH_HOST=https://your-cluster.elastic-cloud.com
ELASTICSEARCH_PORT=9243
ELASTICSEARCH_USERNAME=elastic
ELASTICSEARCH_PASSWORD=your-password
EOF
```

### 4. Build Docker Image

```bash
docker build -t es-rerank .
```

### 5. Run Reranking

**Dry run (preview only):**
```bash
docker run --rm \
  --env-file .env \
  -v $(pwd)/data:/data \
  es-rerank \
  python rerank.py --csv /data/metrics.csv --dry-run
```

**Apply changes:**
```bash
docker run --rm \
  --env-file .env \
  -v $(pwd)/data:/data \
  es-rerank \
  python rerank.py --csv /data/metrics.csv --apply
```

## Command Options

```
--csv           Path to CSV file (required)
--sku-column    SKU column name (default: "Sku")
--views-column  Views column name (default: "Item Viewed")
--index         ES index (default: "skus_product_pool_v3")
--max-score     Maximum score (default: 100)
--factor        Decay factor (default: 25)
--dry-run       Preview without updating
--apply         Apply changes to Elasticsearch
```

## Custom Settings

```bash
# Slower decay (high-view products keep more score)
docker run --rm --env-file .env -v $(pwd)/data:/data es-rerank \
  python rerank.py --csv /data/metrics.csv --apply --factor 15

# Different index
docker run --rm --env-file .env -v $(pwd)/data:/data es-rerank \
  python rerank.py --csv /data/metrics.csv --apply --index skus_product_pool_v2
```

## Using Docker Compose

```bash
# Place CSV in ./data/ folder
mkdir -p data
cp metrics.csv data/

# Dry run
docker-compose run rerank python rerank.py --csv /data/metrics.csv --dry-run

# Apply
docker-compose run rerank python rerank.py --csv /data/metrics.csv --apply
```

## Cron Job (Scheduled Reranking)

```bash
# Edit crontab
crontab -e

# Run daily at 3 AM
0 3 * * * docker run --rm --env-file /home/ec2-user/rerank/.env -v /home/ec2-user/rerank/data:/data es-rerank python rerank.py --csv /data/latest_metrics.csv --apply >> /var/log/rerank.log 2>&1
```

## Files

```
elastic/
├── Dockerfile          # Docker image definition
├── docker-compose.yml  # Docker Compose config
├── requirements.txt    # Python dependencies
├── rerank.py          # Main script
├── .env               # Elasticsearch credentials (create this)
└── data/              # Mount point for CSV files
    └── metrics.csv
```
