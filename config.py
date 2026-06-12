# Project configuration

from pathlib import Path

PROJECT_ROOT = Path.cwd()

# Input files
INPUT_DIR = PROJECT_ROOT / "files"
SALES_ORDER_DETAIL_PATH = str(INPUT_DIR / "sales-order-detail.csv")
SALES_ORDER_HEADER_PATH = str(INPUT_DIR / "sales-order-header.csv")
PRODUCTS_PATH = str(INPUT_DIR / "products.csv")

# Local Parquet output. Each folder below works as a logical table.
BASE_PARQUET_PATH = str(PROJECT_ROOT / "parquet_tables")
SPARK_WAREHOUSE_PATH = str(PROJECT_ROOT / "spark-warehouse")

# Quality behavior for local assessment execution.
# Keep False so the reviewer can run the whole project and inspect reports/quarantine.
# In production this would usually be True for CRITICAL failures.
FAIL_ON_CRITICAL = False

BRONZE_LAYER = "bronze"
SILVER_LAYER = "silver"
GOLD_LAYER = "gold"
QUARANTINE_LAYER = "quarantine"

BRONZE_TABLES = {
    "sales_order_detail": "raw_sales_order_detail",
    "sales_order_header": "raw_sales_order_header",
    "products": "raw_products",
}

SILVER_TABLES = {
    "sales_order_detail": "store_sales_order_detail",
    "sales_order_header": "store_sales_order_header",
    "products_typed": "store_products_typed",
    "products": "store_products",
    "quality_report": "quality_report",
}

GOLD_TABLES = {
    "publish_product": "publish_product",
    "publish_orders": "publish_orders",
    "quality_report": "quality_report",
    "highest_revenue_color_by_year": "answer_highest_revenue_color_by_year",
    "avg_lead_time_by_category": "answer_avg_lead_time_by_category",
}
