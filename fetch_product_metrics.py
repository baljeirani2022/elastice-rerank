import os
import csv
from dotenv import load_dotenv
import redshift_connector

load_dotenv()

def get_connection():
    return redshift_connector.connect(
        iam=True,
        host=os.getenv('REDSHIFT_HOST'),
        port=int(os.getenv('REDSHIFT_PORT', 5439)),
        database=os.getenv('REDSHIFT_DATABASE'),
        db_user=os.getenv('REDSHIFT_USER'),
        access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region=os.getenv('AWS_REGION'),
        cluster_identifier='jazi-datawarehouse-cluster'
    )

def fetch_product_metrics():
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

    conn = get_connection()
    cursor = conn.cursor()

    print("Executing query...")
    cursor.execute(query)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    return columns, rows

def save_to_csv(columns, rows, filename='product_metrics_live.csv'):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"Exported {len(rows)} rows to {filename}")

if __name__ == '__main__':
    columns, rows = fetch_product_metrics()
    save_to_csv(columns, rows)
