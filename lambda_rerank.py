#!/usr/bin/env python3
"""
AWS Lambda function for reranking products.
Triggered by EventBridge/CloudWatch scheduled rule.

Environment Variables:
    ELASTICSEARCH_HOST     - Elasticsearch host URL
    ELASTICSEARCH_PORT     - Elasticsearch port (default: 9243)
    ELASTICSEARCH_USERNAME - Elasticsearch username
    ELASTICSEARCH_PASSWORD - Elasticsearch password
    REDSHIFT_HOST          - Redshift cluster host
    REDSHIFT_PORT          - Redshift port (default: 5439)
    REDSHIFT_DATABASE      - Redshift database name
    REDSHIFT_USER          - Redshift user
    REDSHIFT_CLUSTER_ID    - Redshift cluster identifier
    AWS_REGION             - AWS region (set automatically by Lambda)
    ES_INDEX               - Elasticsearch index (default: skus_product_pool_v3)
    MAX_SCORE              - Maximum trending score (default: 100)
    FACTOR                 - Decay factor (default: 30)
"""

import os
import json
import numpy as np
import pandas as pd
import redshift_connector
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def get_es_client():
    """Create Elasticsearch client."""
    return Elasticsearch(
        hosts=[f"{os.environ['ELASTICSEARCH_HOST']}:{os.environ.get('ELASTICSEARCH_PORT', '9243')}"],
        basic_auth=(os.environ['ELASTICSEARCH_USERNAME'], os.environ['ELASTICSEARCH_PASSWORD'])
    )


def get_redshift_connection():
    """Create Redshift connection using IAM authentication."""
    return redshift_connector.connect(
        iam=True,
        host=os.environ['REDSHIFT_HOST'],
        port=int(os.environ.get('REDSHIFT_PORT', 5439)),
        database=os.environ['REDSHIFT_DATABASE'],
        db_user=os.environ['REDSHIFT_USER'],
        region=os.environ.get('AWS_REGION', 'me-south-1'),
        cluster_identifier=os.environ.get('REDSHIFT_CLUSTER_ID', 'jazi-datawarehouse-cluster')
    )


def calculate_trending_score(views: float, max_score: float = 100, factor: float = 30) -> float:
    """
    Calculate trending score using logarithmic decay.
    Low views = high score (promoted), high views = low score (demoted).
    """
    score = max_score - (np.log10(views + 1) * factor)
    return max(0, min(max_score, score))


def lambda_handler(event, context):
    """
    Lambda handler for reranking products.

    Can be triggered by:
    - EventBridge scheduled rule (cron)
    - API Gateway (optional)
    - Direct invocation

    Event can contain:
    - max_score: Maximum score value (default: 100)
    - factor: Decay factor (default: 30)
    - dry_run: If true, only preview changes (default: false)
    - index: Elasticsearch index to update
    """
    # Get parameters from event or environment
    max_score = float(event.get('max_score', os.environ.get('MAX_SCORE', 100)))
    factor = float(event.get('factor', os.environ.get('FACTOR', 30)))
    dry_run = event.get('dry_run', False)
    index = event.get('index', os.environ.get('ES_INDEX', 'skus_product_pool_v3'))

    print(f"Starting rerank: index={index}, max_score={max_score}, factor={factor}, dry_run={dry_run}")

    try:
        # Step 1: Fetch data from Redshift
        print("Connecting to Redshift...")
        conn = get_redshift_connection()

        query = """
        SELECT sku, item_viewed
        FROM product_reports.product_metrics
        WHERE pushed_status = 'Completed'
          AND app_status = 'Live'
        """

        df = pd.read_sql(query, conn)
        conn.close()
        print(f"Fetched {len(df)} products from Redshift")

        if df.empty:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'status': 'error',
                    'message': 'No products found in Redshift'
                })
            }

        # Step 2: Calculate trending scores
        df['trending_score'] = df['item_viewed'].apply(
            lambda v: calculate_trending_score(v, max_score, factor)
        )

        # Stats
        stats = {
            'total_products': len(df),
            'score_min': round(df['trending_score'].min(), 2),
            'score_max': round(df['trending_score'].max(), 2),
            'score_avg': round(df['trending_score'].mean(), 2),
            'views_min': int(df['item_viewed'].min()),
            'views_max': int(df['item_viewed'].max()),
            'views_avg': round(df['item_viewed'].mean(), 2)
        }

        # Preview samples
        preview = {
            'lowest_views': df.nsmallest(5, 'item_viewed')[['sku', 'item_viewed', 'trending_score']].to_dict('records'),
            'highest_views': df.nlargest(5, 'item_viewed')[['sku', 'item_viewed', 'trending_score']].to_dict('records')
        }

        if dry_run:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'dry_run',
                    'message': 'Preview only - no changes made',
                    'index': index,
                    'formula': f'score = {max_score} - log10(views + 1) * {factor}',
                    'stats': stats,
                    'preview': preview
                })
            }

        # Step 3: Update Elasticsearch
        print("Connecting to Elasticsearch...")
        es = get_es_client()

        def generate_actions():
            for _, row in df.iterrows():
                yield {
                    '_op_type': 'update',
                    '_index': index,
                    '_id': row['sku'],
                    'doc': {
                        'trending_score': row['trending_score'],
                        'views_count': int(row['item_viewed'])
                    }
                }

        print("Updating Elasticsearch...")
        success, failed = bulk(es, generate_actions(), chunk_size=500, raise_on_error=False)
        failed_count = len(failed) if isinstance(failed, list) else failed

        print(f"Completed: {success} updated, {failed_count} failed")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'completed',
                'message': 'Reranking completed successfully',
                'index': index,
                'formula': f'score = {max_score} - log10(views + 1) * {factor}',
                'stats': stats,
                'results': {
                    'updated': success,
                    'failed': failed_count
                },
                'preview': preview
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e)
            })
        }


# For local testing
if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()

    # Test with dry run
    result = lambda_handler({'dry_run': True, 'max_score': 100, 'factor': 30}, None)
    print(json.dumps(json.loads(result['body']), indent=2))
