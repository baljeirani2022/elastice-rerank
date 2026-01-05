#!/usr/bin/env python3
"""
Reranking Analytics API (Secured)
Provides endpoints for viewing trending score distribution and analytics.

Usage:
    python api.py

Environment Variables:
    API_KEY         - Required API key for authentication
    API_PORT        - Port to run on (default: 5000)
    ES_INDEX        - Default Elasticsearch index

Endpoints:
    GET /health              - Health check (no auth required)
    GET /stats               - Overall statistics
    GET /distribution/views  - Products by view ranges
    GET /distribution/scores - Products by score ranges
    GET /top                 - Top trending products
    GET /bottom              - Bottom trending products
    GET /summary             - Full summary
"""

import os
import numpy as np
import pandas as pd
import redshift_connector
from functools import wraps
from flask import Flask, jsonify, request
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

load_dotenv()

app = Flask(__name__)

# API Key from environment
API_KEY = os.getenv('API_KEY', 'change-me-in-production')

def get_es_client():
    """Create Elasticsearch client."""
    return Elasticsearch(
        hosts=[f"{os.getenv('ELASTICSEARCH_HOST')}:{os.getenv('ELASTICSEARCH_PORT')}"],
        basic_auth=(os.getenv('ELASTICSEARCH_USERNAME'), os.getenv('ELASTICSEARCH_PASSWORD'))
    )

# Default index
DEFAULT_INDEX = os.getenv('ES_INDEX', 'skus_product_pool_v3')


def require_api_key(f):
    """Decorator to require API key authentication."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check header first, then query param
        api_key = request.headers.get('X-API-Key') or request.args.get('api_key')

        if not api_key:
            return jsonify({
                "error": "Missing API key",
                "message": "Provide API key via X-API-Key header or api_key query parameter"
            }), 401

        if api_key != API_KEY:
            return jsonify({
                "error": "Invalid API key",
                "message": "The provided API key is not valid"
            }), 403

        return f(*args, **kwargs)
    return decorated_function


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint (no auth required)."""
    try:
        es = get_es_client()
        info = es.info()
        return jsonify({
            "status": "healthy",
            "elasticsearch": {
                "cluster": info['cluster_name'],
                "version": info['version']['number']
            }
        })
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


@app.route('/stats', methods=['GET'])
@require_api_key
def stats():
    """Overall statistics for trending scores."""
    index = request.args.get('index', DEFAULT_INDEX)

    es = get_es_client()

    result = es.search(
        index=index,
        size=0,
        aggs={
            "trending_stats": {
                "stats": {"field": "trending_score"}
            },
            "views_stats": {
                "stats": {"field": "views_count"}
            },
            "total_products": {
                "value_count": {"field": "sk.keyword"}
            }
        }
    )

    trending = result['aggregations']['trending_stats']
    views = result['aggregations']['views_stats']

    return jsonify({
        "index": index,
        "total_products": int(trending['count']),
        "trending_score": {
            "min": round(trending['min'], 2) if trending['min'] else 0,
            "max": round(trending['max'], 2) if trending['max'] else 0,
            "avg": round(trending['avg'], 2) if trending['avg'] else 0
        },
        "views": {
            "min": int(views['min']) if views['min'] else 0,
            "max": int(views['max']) if views['max'] else 0,
            "avg": round(views['avg'], 2) if views['avg'] else 0
        }
    })


@app.route('/distribution/views', methods=['GET'])
@require_api_key
def distribution_views():
    """Products distribution by view ranges."""
    index = request.args.get('index', DEFAULT_INDEX)

    es = get_es_client()

    result = es.search(
        index=index,
        size=0,
        aggs={
            "view_ranges": {
                "range": {
                    "field": "views_count",
                    "ranges": [
                        {"key": "0-100", "from": 0, "to": 100},
                        {"key": "100-200", "from": 100, "to": 200},
                        {"key": "200-500", "from": 200, "to": 500},
                        {"key": "500-1000", "from": 500, "to": 1000},
                        {"key": "1000-2000", "from": 1000, "to": 2000},
                        {"key": "2000-5000", "from": 2000, "to": 5000},
                        {"key": "5000-10000", "from": 5000, "to": 10000},
                        {"key": "10000+", "from": 10000}
                    ]
                },
                "aggs": {
                    "avg_score": {"avg": {"field": "trending_score"}}
                }
            },
            "total": {
                "value_count": {"field": "views_count"}
            }
        }
    )

    total = result['aggregations']['total']['value']
    buckets = result['aggregations']['view_ranges']['buckets']

    distribution = []
    for bucket in buckets:
        count = bucket['doc_count']
        pct = round((count / total * 100), 2) if total > 0 else 0
        avg_score = bucket['avg_score']['value']

        distribution.append({
            "range": bucket['key'],
            "count": count,
            "percentage": pct,
            "avg_trending_score": round(avg_score, 2) if avg_score else 0
        })

    return jsonify({
        "index": index,
        "total_products": total,
        "distribution": distribution
    })


@app.route('/distribution/scores', methods=['GET'])
@require_api_key
def distribution_scores():
    """Products distribution by trending score ranges."""
    index = request.args.get('index', DEFAULT_INDEX)

    es = get_es_client()

    result = es.search(
        index=index,
        size=0,
        aggs={
            "score_ranges": {
                "range": {
                    "field": "trending_score",
                    "ranges": [
                        {"key": "0-10 (Very Low)", "from": 0, "to": 10},
                        {"key": "10-25 (Low)", "from": 10, "to": 25},
                        {"key": "25-50 (Medium-Low)", "from": 25, "to": 50},
                        {"key": "50-75 (Medium-High)", "from": 50, "to": 75},
                        {"key": "75-90 (High)", "from": 75, "to": 90},
                        {"key": "90-100 (Very High)", "from": 90, "to": 101}
                    ]
                },
                "aggs": {
                    "avg_views": {"avg": {"field": "views_count"}}
                }
            },
            "total": {
                "value_count": {"field": "trending_score"}
            }
        }
    )

    total = result['aggregations']['total']['value']
    buckets = result['aggregations']['score_ranges']['buckets']

    distribution = []
    for bucket in buckets:
        count = bucket['doc_count']
        pct = round((count / total * 100), 2) if total > 0 else 0
        avg_views = bucket['avg_views']['value']

        distribution.append({
            "range": bucket['key'],
            "count": count,
            "percentage": pct,
            "avg_views": int(avg_views) if avg_views else 0
        })

    return jsonify({
        "index": index,
        "total_products": total,
        "distribution": distribution
    })


@app.route('/top', methods=['GET'])
@require_api_key
def top_trending():
    """Top trending products (highest scores)."""
    index = request.args.get('index', DEFAULT_INDEX)
    limit = min(int(request.args.get('limit', 20)), 100)  # Max 100

    es = get_es_client()

    result = es.search(
        index=index,
        size=limit,
        sort=[{"trending_score": "desc"}],
        _source=["sk", "name", "trending_score", "views_count", "price", "category"]
    )

    products = []
    for hit in result['hits']['hits']:
        s = hit['_source']
        products.append({
            "sku": s.get('sk'),
            "name": s.get('name'),
            "trending_score": round(s.get('trending_score', 0), 2),
            "views": s.get('views_count', 0),
            "price": s.get('price'),
            "category": s.get('category')
        })

    return jsonify({
        "index": index,
        "count": len(products),
        "products": products
    })


@app.route('/bottom', methods=['GET'])
@require_api_key
def bottom_trending():
    """Bottom trending products (lowest scores)."""
    index = request.args.get('index', DEFAULT_INDEX)
    limit = min(int(request.args.get('limit', 20)), 100)  # Max 100

    es = get_es_client()

    result = es.search(
        index=index,
        size=limit,
        sort=[{"trending_score": "asc"}],
        _source=["sk", "name", "trending_score", "views_count", "price", "category"]
    )

    products = []
    for hit in result['hits']['hits']:
        s = hit['_source']
        products.append({
            "sku": s.get('sk'),
            "name": s.get('name'),
            "trending_score": round(s.get('trending_score', 0), 2),
            "views": s.get('views_count', 0),
            "price": s.get('price'),
            "category": s.get('category')
        })

    return jsonify({
        "index": index,
        "count": len(products),
        "products": products
    })


@app.route('/summary', methods=['GET'])
@require_api_key
def summary():
    """Complete summary with all distributions."""
    index = request.args.get('index', DEFAULT_INDEX)

    es = get_es_client()

    result = es.search(
        index=index,
        size=0,
        aggs={
            "trending_stats": {
                "stats": {"field": "trending_score"}
            },
            "view_ranges": {
                "range": {
                    "field": "views_count",
                    "ranges": [
                        {"key": "0-100", "from": 0, "to": 100},
                        {"key": "100-200", "from": 100, "to": 200},
                        {"key": "200-500", "from": 200, "to": 500},
                        {"key": "500-1000", "from": 500, "to": 1000},
                        {"key": "1000-2000", "from": 1000, "to": 2000},
                        {"key": "2000-5000", "from": 2000, "to": 5000},
                        {"key": "5000-10000", "from": 5000, "to": 10000},
                        {"key": "10000+", "from": 10000}
                    ]
                },
                "aggs": {
                    "avg_score": {"avg": {"field": "trending_score"}}
                }
            },
            "score_ranges": {
                "range": {
                    "field": "trending_score",
                    "ranges": [
                        {"key": "0-25 (Low)", "from": 0, "to": 25},
                        {"key": "25-50 (Medium-Low)", "from": 25, "to": 50},
                        {"key": "50-75 (Medium-High)", "from": 50, "to": 75},
                        {"key": "75-100 (High)", "from": 75, "to": 101}
                    ]
                }
            }
        }
    )

    stats = result['aggregations']['trending_stats']
    total = int(stats['count'])

    # View distribution
    view_dist = []
    for bucket in result['aggregations']['view_ranges']['buckets']:
        count = bucket['doc_count']
        avg_score = bucket['avg_score']['value']
        view_dist.append({
            "range": bucket['key'],
            "count": count,
            "percentage": round((count / total * 100), 2) if total > 0 else 0,
            "avg_score": round(avg_score, 2) if avg_score else 0
        })

    # Score distribution
    score_dist = []
    for bucket in result['aggregations']['score_ranges']['buckets']:
        count = bucket['doc_count']
        score_dist.append({
            "range": bucket['key'],
            "count": count,
            "percentage": round((count / total * 100), 2) if total > 0 else 0
        })

    return jsonify({
        "index": index,
        "total_products": total,
        "trending_score": {
            "min": round(stats['min'], 2) if stats['min'] else 0,
            "max": round(stats['max'], 2) if stats['max'] else 0,
            "avg": round(stats['avg'], 2) if stats['avg'] else 0
        },
        "by_views": view_dist,
        "by_score": score_dist
    })


def get_redshift_connection():
    """Create Redshift connection using IAM authentication."""
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


def calculate_trending_score(views: float, max_score: float = 100, factor: float = 25) -> float:
    """
    Calculate trending score using logarithmic decay.
    Low views = high score (promoted), high views = low score (demoted).
    """
    score = max_score - (np.log10(views + 1) * factor)
    return max(0, min(max_score, score))


@app.route('/rerank', methods=['POST'])
@require_api_key
def rerank():
    """
    Trigger reranking of products based on view counts from Redshift.

    Query params:
        - dry_run: If 'true', only preview changes without updating (default: false)
        - max_score: Maximum score value (default: 100)
        - factor: Decay factor for logarithmic formula (default: 25)
        - index: Elasticsearch index to update (default: skus_product_pool_v3)
    """
    dry_run = request.args.get('dry_run', 'false').lower() == 'true'
    max_score = float(request.args.get('max_score', 100))
    factor = float(request.args.get('factor', 25))
    index = request.args.get('index', DEFAULT_INDEX)

    try:
        # Step 1: Fetch data from Redshift
        conn = get_redshift_connection()

        query = """
        SELECT sku, item_viewed
        FROM product_reports.product_metrics
        WHERE pushed_status = 'Completed'
          AND app_status = 'Live'
        """

        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            return jsonify({
                "status": "error",
                "message": "No products found in Redshift"
            }), 404

        # Step 2: Calculate trending scores
        df['trending_score'] = df['item_viewed'].apply(
            lambda v: calculate_trending_score(v, max_score, factor)
        )

        # Stats
        stats = {
            "total_products": len(df),
            "score_min": round(df['trending_score'].min(), 2),
            "score_max": round(df['trending_score'].max(), 2),
            "score_avg": round(df['trending_score'].mean(), 2),
            "views_min": int(df['item_viewed'].min()),
            "views_max": int(df['item_viewed'].max()),
            "views_avg": round(df['item_viewed'].mean(), 2)
        }

        # Preview samples
        preview = {
            "lowest_views": df.nsmallest(5, 'item_viewed')[['sku', 'item_viewed', 'trending_score']].to_dict('records'),
            "highest_views": df.nlargest(5, 'item_viewed')[['sku', 'item_viewed', 'trending_score']].to_dict('records')
        }

        if dry_run:
            return jsonify({
                "status": "dry_run",
                "message": "Preview only - no changes made",
                "index": index,
                "formula": f"score = {max_score} - log10(views + 1) * {factor}",
                "stats": stats,
                "preview": preview
            })

        # Step 3: Update Elasticsearch
        es = get_es_client()

        def generate_actions():
            for _, row in df.iterrows():
                yield {
                    "_op_type": "update",
                    "_index": index,
                    "_id": row['sku'],
                    "doc": {
                        "trending_score": row['trending_score'],
                        "views_count": int(row['item_viewed'])
                    }
                }

        success, failed = bulk(es, generate_actions(), chunk_size=500, raise_on_error=False)
        failed_count = len(failed) if isinstance(failed, list) else failed

        return jsonify({
            "status": "completed",
            "message": f"Reranking completed successfully",
            "index": index,
            "formula": f"score = {max_score} - log10(views + 1) * {factor}",
            "stats": stats,
            "results": {
                "updated": success,
                "failed": failed_count
            },
            "preview": preview
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/sku/<sku_id>', methods=['GET'])
@require_api_key
def get_sku(sku_id):
    """
    Get product details by SKU ID.

    Returns product info from both Elasticsearch and Redshift.
    """
    index = request.args.get('index', DEFAULT_INDEX)
    include_redshift = request.args.get('include_redshift', 'true').lower() == 'true'

    try:
        es = get_es_client()

        # Search in Elasticsearch
        result = es.search(
            index=index,
            query={"term": {"sk.keyword": sku_id}},
            size=1
        )

        if result['hits']['total']['value'] == 0:
            # Try direct doc lookup
            try:
                doc = es.get(index=index, id=sku_id)
                es_data = doc['_source']
            except:
                es_data = None
        else:
            es_data = result['hits']['hits'][0]['_source']

        response = {
            "sku": sku_id,
            "elasticsearch": es_data
        }

        # Optionally fetch from Redshift
        if include_redshift:
            try:
                conn = get_redshift_connection()
                query = f"""
                SELECT *
                FROM product_reports.product_metrics
                WHERE sku = '{sku_id}'
                """
                df = pd.read_sql(query, conn)
                conn.close()

                if not df.empty:
                    response["redshift"] = df.iloc[0].to_dict()
                else:
                    response["redshift"] = None
            except Exception as e:
                response["redshift_error"] = str(e)

        return jsonify(response)

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/download', methods=['GET'])
@require_api_key
def download_metrics():
    """
    Download current product metrics from Redshift as CSV.

    Query params:
        - format: 'json' or 'csv' (default: csv)
    """
    from flask import Response
    from datetime import datetime

    output_format = request.args.get('format', 'csv').lower()

    try:
        conn = get_redshift_connection()

        query = """
        SELECT
            sku,
            name,
            catalog_tag,
            catalog_layer1,
            catalog_layer2,
            pushed_status,
            app_status,
            item_viewed,
            users_viewed_item,
            item_added_to_bag,
            users_ordered,
            quantity_ordered,
            product_revenue,
            product_profit,
            views_score,
            conversion_score,
            total_score_raw,
            rank_overall
        FROM product_reports.product_metrics
        WHERE pushed_status = 'Completed'
          AND app_status = 'Live'
        ORDER BY item_viewed DESC
        """

        df = pd.read_sql(query, conn)
        conn.close()

        if output_format == 'json':
            return jsonify({
                "status": "success",
                "count": len(df),
                "data": df.to_dict('records')
            })

        # CSV format
        csv_data = df.to_csv(index=False)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

        return Response(
            csv_data,
            mimetype='text/csv',
            headers={
                'Content-Disposition': f'attachment; filename=product_metrics_{timestamp}.csv'
            }
        )

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found", "message": "The requested endpoint does not exist"}), 404


@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Server error", "message": str(e)}), 500


if __name__ == '__main__':
    port = int(os.getenv('API_PORT', 5000))

    # Check if API_KEY is set
    if API_KEY == 'change-me-in-production':
        print("\n⚠️  WARNING: Using default API key. Set API_KEY environment variable in production!\n")

    print(f"""
╔══════════════════════════════════════════════════════════╗
║           Reranking Analytics API (Secured)              ║
╠══════════════════════════════════════════════════════════╣
║  Authentication: X-API-Key header or ?api_key= param    ║
╠══════════════════════════════════════════════════════════╣
║  Analytics Endpoints:                                    ║
║    GET  /health              - Health check (no auth)    ║
║    GET  /stats               - Overall stats             ║
║    GET  /distribution/views  - By view ranges            ║
║    GET  /distribution/scores - By score ranges           ║
║    GET  /top?limit=20        - Top trending              ║
║    GET  /bottom?limit=20     - Bottom trending           ║
║    GET  /summary             - Full summary              ║
║    GET  /sku/<sku_id>        - Get SKU details           ║
╠══════════════════════════════════════════════════════════╣
║  Action Endpoints:                                       ║
║    POST /rerank              - Trigger reranking         ║
║    POST /rerank?dry_run=true - Preview reranking         ║
║    GET  /download            - Download metrics CSV      ║
╚══════════════════════════════════════════════════════════╝
    """)
    app.run(host='0.0.0.0', port=port, debug=False)
