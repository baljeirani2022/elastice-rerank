#!/usr/bin/env python3
"""
Product Reranking Script for Elasticsearch
Promotes low-view products and demotes high-view products based on logarithmic decay.

Usage:
    python rerank.py --csv /path/to/metrics.csv --dry-run
    python rerank.py --csv /path/to/metrics.csv --apply
"""

import os
import sys
import argparse
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def get_es_client():
    """Create Elasticsearch client from environment variables."""
    load_dotenv()

    host = os.getenv('ELASTICSEARCH_HOST')
    port = os.getenv('ELASTICSEARCH_PORT')
    username = os.getenv('ELASTICSEARCH_USERNAME')
    password = os.getenv('ELASTICSEARCH_PASSWORD')

    if not all([host, port, username, password]):
        raise ValueError("Missing Elasticsearch credentials. Set ELASTICSEARCH_HOST, ELASTICSEARCH_PORT, ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD")

    return Elasticsearch(
        hosts=[f"{host}:{port}"],
        basic_auth=(username, password)
    )


def calculate_trending_score(views: float, max_score: float = 100, factor: float = 25) -> float:
    """
    Calculate trending score using logarithmic decay.
    Low views = high score, high views = low score.

    Formula: score = max_score - log10(views + 1) * factor

    Example scores with default settings:
        0 views    → 100
        10 views   → 74
        100 views  → 50
        1000 views → 25
        10000 views → 0
    """
    score = max_score - (np.log10(views + 1) * factor)
    return max(0, min(max_score, score))


def rerank_from_csv(
    csv_path: str,
    sku_column: str = "Sku",
    views_column: str = "Item Viewed",
    index: str = "skus_product_pool_v3",
    max_score: float = 100,
    factor: float = 25,
    dry_run: bool = True
) -> dict:
    """
    Rerank products based on views from a CSV file.
    """
    # Connect to Elasticsearch
    es = get_es_client()

    # Test connection
    info = es.info()
    print(f"Connected to Elasticsearch: {info['cluster_name']} (v{info['version']['number']})")

    # Read CSV
    df = pd.read_csv(csv_path, low_memory=False)

    # Clean views column (remove commas if present)
    df['_views'] = df[views_column].astype(str).str.replace(',', '').astype(float)

    # Calculate new scores
    df['_new_score'] = df['_views'].apply(lambda v: calculate_trending_score(v, max_score, factor))

    print(f"\n{'='*50}")
    print(f"RERANKING SUMMARY")
    print(f"{'='*50}")
    print(f"CSV: {csv_path}")
    print(f"Total products: {len(df):,}")
    print(f"Index: {index}")
    print(f"Formula: score = {max_score} - log10(views + 1) * {factor}")

    print(f"\n--- Score Distribution ---")
    print(f"Min score: {df['_new_score'].min():.2f}")
    print(f"Max score: {df['_new_score'].max():.2f}")
    print(f"Mean score: {df['_new_score'].mean():.2f}")

    print(f"\n--- Preview ---")
    preview = df[[sku_column, '_views', '_new_score']].copy()
    preview.columns = ['SKU', 'Views', 'New Score']
    print("\nLowest views (will be promoted):")
    print(preview.nsmallest(5, 'Views').to_string(index=False))
    print("\nHighest views (will be demoted):")
    print(preview.nlargest(5, 'Views').to_string(index=False))

    if dry_run:
        print(f"\n{'='*50}")
        print("⚠️  DRY RUN - No changes made.")
        print("Run with --apply to update Elasticsearch.")
        print(f"{'='*50}")
        return {"status": "dry_run", "total": len(df)}

    # Bulk update Elasticsearch
    print(f"\n{'='*50}")
    print("UPDATING ELASTICSEARCH")
    print(f"{'='*50}")

    def generate_actions():
        for i, row in df.iterrows():
            yield {
                "_op_type": "update",
                "_index": index,
                "_id": row[sku_column],
                "doc": {"trending_score": row['_new_score']}
            }
            if (i + 1) % 5000 == 0:
                print(f"Prepared {i + 1:,}/{len(df):,}...")

    success, failed = bulk(es, generate_actions(), chunk_size=500, raise_on_error=False)

    failed_count = len(failed) if isinstance(failed, list) else failed

    print(f"\n{'='*50}")
    print("COMPLETE")
    print(f"{'='*50}")
    print(f"✓ Updated: {success:,}")
    print(f"✗ Failed: {failed_count}")

    return {
        "status": "completed",
        "total": len(df),
        "success": success,
        "failed": failed_count
    }


def main():
    parser = argparse.ArgumentParser(
        description="Rerank products in Elasticsearch based on view counts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview changes (dry run)
  python rerank.py --csv metrics.csv --dry-run

  # Apply changes to Elasticsearch
  python rerank.py --csv metrics.csv --apply

  # Custom settings
  python rerank.py --csv metrics.csv --apply --index skus_product_pool_v3 --factor 30
        """
    )

    parser.add_argument('--csv', required=True, help='Path to CSV file with SKU and views data')
    parser.add_argument('--sku-column', default='Sku', help='Column name for SKU (default: Sku)')
    parser.add_argument('--views-column', default='Item Viewed', help='Column name for views (default: Item Viewed)')
    parser.add_argument('--index', default='skus_product_pool_v3', help='Elasticsearch index (default: skus_product_pool_v3)')
    parser.add_argument('--max-score', type=float, default=100, help='Maximum score (default: 100)')
    parser.add_argument('--factor', type=float, default=25, help='Decay factor (default: 25)')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dry-run', action='store_true', help='Preview changes without updating')
    group.add_argument('--apply', action='store_true', help='Apply changes to Elasticsearch')

    args = parser.parse_args()

    result = rerank_from_csv(
        csv_path=args.csv,
        sku_column=args.sku_column,
        views_column=args.views_column,
        index=args.index,
        max_score=args.max_score,
        factor=args.factor,
        dry_run=args.dry_run
    )

    sys.exit(0 if result.get('status') in ['dry_run', 'completed'] else 1)


if __name__ == '__main__':
    main()
