#!/usr/bin/env python3
"""
Product Reranking Pipeline
Fetches product metrics from Redshift and updates trending scores in Elasticsearch.

Usage:
    python rerank_pipeline.py --dry-run     # Preview without updating
    python rerank_pipeline.py --apply       # Fetch and update Elasticsearch
    python rerank_pipeline.py --csv-only    # Only generate CSV, don't update ES
"""

import os
import sys
import csv
import argparse
from datetime import datetime
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import redshift_connector
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def get_redshift_connection():
    """Create Redshift connection from environment variables."""
    return redshift_connector.connect(
        iam=True,
        host=os.getenv('REDSHIFT_HOST'),
        port=int(os.getenv('REDSHIFT_PORT', 5439)),
        database=os.getenv('REDSHIFT_DATABASE'),
        db_user=os.getenv('REDSHIFT_USER'),
        access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region=os.getenv('AWS_REGION'),
        cluster_identifier=os.getenv('REDSHIFT_CLUSTER_ID', 'jazi-datawarehouse-cluster')
    )


def get_es_client():
    """Create Elasticsearch client from environment variables."""
    host = os.getenv('ELASTICSEARCH_HOST')
    port = os.getenv('ELASTICSEARCH_PORT')
    username = os.getenv('ELASTICSEARCH_USERNAME')
    password = os.getenv('ELASTICSEARCH_PASSWORD')

    if not all([host, port, username, password]):
        raise ValueError("Missing Elasticsearch credentials")

    return Elasticsearch(
        hosts=[f"{host}:{port}"],
        basic_auth=(username, password)
    )


def fetch_product_metrics():
    """Fetch product metrics from Redshift."""
    query = """
    SELECT
        sku,
        name,
        catalog_tag,
        catalog_layer1,
        catalog_layer2,
        catalog_layer3,
        catalog_layer4,
        pushed_status,
        app_status,
        supplier_id,
        supplier_name,
        cost_sar,
        date_created,
        final_status_submitted_on,
        item_viewed,
        users_viewed_item,
        item_added_to_bag,
        users_added_to_bag,
        users_ordered,
        quantity_ordered,
        number_of_orders,
        gm_pct,
        product_revenue,
        product_profit,
        fail_count,
        out_of_stock_count,
        return_count,
        viewed_to_ordered,
        viewed_to_added_to_bag,
        added_to_bag_to_ordered,
        added_to_bag_to_ordered_by_quantity,
        views_score,
        conversion_score,
        total_score_raw,
        rank_overall
    FROM product_reports.product_metrics
    WHERE pushed_status = 'Completed'
      AND app_status = 'Live'
    ORDER BY item_viewed DESC
    """

    print("Connecting to Redshift...")
    conn = get_redshift_connection()
    cursor = conn.cursor()

    print("Fetching product metrics...")
    cursor.execute(query)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=columns)
    print(f"Fetched {len(df):,} products from Redshift")

    return df


def calculate_trending_score(views: float, max_score: float = 100, factor: float = 25) -> float:
    """
    Calculate trending score using logarithmic decay.
    Low views = high score, high views = low score.
    """
    if pd.isna(views):
        views = 0
    score = max_score - (np.log10(views + 1) * factor)
    return max(0, min(max_score, score))


def save_to_csv(df, filename=None):
    """Save DataFrame to CSV with timestamp."""
    if filename is None:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = f"product_metrics_{timestamp}.csv"

    df.to_csv(filename, index=False)
    print(f"Saved to: {filename}")
    return filename


def update_elasticsearch(df, index='skus_product_pool_v3', dry_run=True):
    """Update trending scores in Elasticsearch."""
    es = get_es_client()

    # Test connection
    info = es.info()
    print(f"Connected to Elasticsearch: {info['cluster_name']} (v{info['version']['number']})")

    # Calculate new scores
    df['_views'] = pd.to_numeric(df['item_viewed'], errors='coerce').fillna(0)
    df['_new_score'] = df['_views'].apply(calculate_trending_score)

    print(f"\n{'='*50}")
    print("RERANKING SUMMARY")
    print(f"{'='*50}")
    print(f"Total products: {len(df):,}")
    print(f"Index: {index}")
    print(f"Formula: score = 100 - log10(views + 1) * 25")

    print(f"\n--- Score Distribution ---")
    print(f"Min score: {df['_new_score'].min():.2f}")
    print(f"Max score: {df['_new_score'].max():.2f}")
    print(f"Mean score: {df['_new_score'].mean():.2f}")

    print(f"\n--- Preview ---")
    preview = df[['sku', '_views', '_new_score']].copy()
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

    # Bulk update
    print(f"\n{'='*50}")
    print("UPDATING ELASTICSEARCH")
    print(f"{'='*50}")

    def generate_actions():
        for i, row in df.iterrows():
            yield {
                "_op_type": "update",
                "_index": index,
                "_id": row['sku'],
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
        description="Fetch product metrics from Redshift and update Elasticsearch trending scores",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview changes (dry run)
  python rerank_pipeline.py --dry-run

  # Apply changes to Elasticsearch
  python rerank_pipeline.py --apply

  # Only generate CSV, don't update ES
  python rerank_pipeline.py --csv-only

  # Custom index
  python rerank_pipeline.py --apply --index skus_product_pool_v3
        """
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dry-run', action='store_true', help='Preview changes without updating')
    group.add_argument('--apply', action='store_true', help='Apply changes to Elasticsearch')
    group.add_argument('--csv-only', action='store_true', help='Only generate CSV file')

    parser.add_argument('--index', default='skus_product_pool_v3', help='Elasticsearch index')
    parser.add_argument('--output', help='Output CSV filename')

    args = parser.parse_args()

    # Load environment
    load_dotenv()

    print(f"\n{'='*50}")
    print("PRODUCT RERANKING PIPELINE")
    print(f"{'='*50}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Step 1: Fetch from Redshift
    df = fetch_product_metrics()

    # Step 2: Save CSV
    csv_file = save_to_csv(df, args.output)

    # Step 3: Update Elasticsearch (unless csv-only)
    if args.csv_only:
        print("\n✓ CSV generated. Skipping Elasticsearch update.")
        return

    result = update_elasticsearch(
        df,
        index=args.index,
        dry_run=args.dry_run
    )

    print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sys.exit(0 if result.get('status') in ['dry_run', 'completed'] else 1)


if __name__ == '__main__':
    main()
