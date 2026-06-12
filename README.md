# Sales Data Engineering Assessment - Medallion Architecture - Local Parquet Version

This project implements the Data Engineering tech assessment using PySpark and local Parquet files.

The project avoids local Spark managed tables, Hive metastore, and Delta dependencies to keep execution simple in VSCode/Jupyter on Windows. Each Parquet folder is treated as a logical table.

## Original Requirements Covered

- Load three files using SQL or PySpark.
- Name raw objects with a `raw_` prefix.
- Review and apply appropriate data types.
- Identify primary and foreign keys.
- Store transformed objects with a `store_` prefix.
- Create `publish_product`.
- Create `publish_orders`.
- Calculate `LeadTimeInBusinessDays`, excluding Saturdays and Sundays.
- Calculate `TotalLineExtendedPrice`.
- Answer:
  - Which color generated the highest revenue each year?
  - What is the average `LeadTimeInBusinessDays` by `ProductCategoryName`?
- Check negative dates.
- Use Medallion Architecture.
- Keep Bronze, Silver, Gold and quality checks in separate files.
- Add Senior-style quality checks and quarantine invalid records.

## Architecture

```text
Bronze -> Silver -> Gold
```

Mapping to the original assessment:

| Original requirement | Local Medallion implementation |
|---|---|
| `raw_` tables | `parquet_tables/bronze/raw_*` |
| `store_` tables | `parquet_tables/silver/store_*` |
| `publish_*` tables | `parquet_tables/gold/publish_*` |
| Invalid records | `parquet_tables/quarantine/*` |

## Project Structure

```text
.
├── setup.py
├── config.py
├── utils.py
├── 00_quality_checks.ipynb
├── 01_bronze_layer.ipynb
├── 02_silver_layer.ipynb
├── 03_gold_layer.ipynb
├── 04_run_all_pipeline.ipynb
├── requirements.txt
├── .gitignore
└── README.md
```

## Input Files

Place the three source files inside a local `files/` folder in the project root:

```text
files/
├── sales-order-detail.csv
├── sales-order-header.csv
└── products.csv
```

Default paths are defined in `config.py`:

```python
SALES_ORDER_DETAIL_PATH = "files/sales-order-detail.csv"
SALES_ORDER_HEADER_PATH = "files/sales-order-header.csv"
PRODUCTS_PATH = "files/products.csv"
```

## Installation

Create and activate a virtual environment, then install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Minimum dependencies:

```text
pyspark==3.5.1
pandas
python-dateutil
ipykernel
```

## Java / Spark Local Notes

The local Spark setup is centralized in `setup.py`.

The fallback `JAVA_HOME` is:

```text
C:\Users\bruna.martins\java\jdk-17.0.19+10
```

If your JDK is installed somewhere else, either:

1. Set `JAVA_HOME` in your machine, or
2. Update the fallback path inside `setup.py`.

## How to Run

### Option 1 - Full pipeline

Open and run:

```text
04_run_all_pipeline.ipynb
```

### Option 2 - Layer by layer

Run the notebooks in this order:

```text
00_quality_checks.ipynb
01_bronze_layer.ipynb
02_silver_layer.ipynb
03_gold_layer.ipynb
```

## Output Layout

The project writes local Parquet datasets as logical tables:

```text
parquet_tables/
├── bronze/
│   ├── raw_sales_order_detail/
│   ├── raw_sales_order_header/
│   └── raw_products/
├── silver/
│   ├── store_sales_order_detail/
│   ├── store_sales_order_header/
│   ├── store_products_typed/
│   ├── store_products/
│   └── quality_report/
├── gold/
│   ├── publish_product/
│   ├── publish_orders/
│   ├── answer_highest_revenue_color_by_year/
│   ├── answer_avg_lead_time_by_category/
│   └── quality_report/
└── quarantine/
    └── negative_dates/
```

## Bronze Layer

Notebook:

```text
01_bronze_layer.ipynb
```

Creates:

```text
parquet_tables/bronze/raw_sales_order_detail
parquet_tables/bronze/raw_sales_order_header
parquet_tables/bronze/raw_products
```

Purpose:

- Load source CSV files.
- Keep source columns as strings.
- Add ingestion metadata.
- Persist raw data as Parquet.

## Silver Layer

Notebook:

```text
02_silver_layer.ipynb
```

Creates:

```text
parquet_tables/silver/store_sales_order_detail
parquet_tables/silver/store_sales_order_header
parquet_tables/silver/store_products_typed
parquet_tables/silver/store_products
parquet_tables/silver/quality_report
```

Purpose:

- Apply explicit data types.
- Parse mixed date formats.
- Identify primary and foreign keys.
- Deduplicate products by `ProductID`.
- Run quality checks.
- Quarantine records with `ShipDate < OrderDate`.

## Gold Layer

Notebook:

```text
03_gold_layer.ipynb
```

Creates:

```text
parquet_tables/gold/publish_product
parquet_tables/gold/publish_orders
parquet_tables/gold/answer_highest_revenue_color_by_year
parquet_tables/gold/answer_avg_lead_time_by_category
parquet_tables/gold/quality_report
```

Purpose:

- Apply product master business rules.
- Create `publish_product`.
- Create `publish_orders`.
- Calculate `LeadTimeInBusinessDays`.
- Calculate `TotalLineExtendedPrice`.
- Answer the analytical questions.
- Run final quality checks.

## Keys

Primary keys:

| Logical table | Primary key |
|---|---|
| `store_sales_order_detail` | `SalesOrderDetailID` |
| `store_sales_order_header` | `SalesOrderID` |
| `store_products` | `ProductID` |
| `publish_product` | `ProductID` |
| `publish_orders` | `SalesOrderDetailID` |

Foreign keys:

| Source | Key | Target |
|---|---|---|
| `store_sales_order_detail` | `SalesOrderID` | `store_sales_order_header.SalesOrderID` |
| `store_sales_order_detail` | `ProductID` | `store_products.ProductID` |
| `publish_orders` | `ProductID` | `publish_product.ProductID` |

## Senior Quality Checks

The project includes reusable quality functions in `00_quality_checks.ipynb`:

- Primary key uniqueness.
- Foreign key integrity.
- Not-null checks.
- Non-negative numeric checks.
- Negative date interval checks.
- Quarantine for invalid date records.
- Critical failure evaluation with configurable behavior.

The main feedback-related validation is:

```text
ShipDate < OrderDate
```

When this happens:

1. The quality report flags the issue.
2. The invalid record is written to `parquet_tables/quarantine/negative_dates/`.
3. `LeadTimeInBusinessDays` is set to `null` in Gold so invalid dates do not contaminate the average lead time metric.

## Product Master Transformations

`publish_product` applies:

- Replace null `Color` with `N/A`.
- Fill missing `ProductCategoryName` using `ProductSubCategoryName`:
  - Clothing: `Gloves`, `Shorts`, `Socks`, `Tights`, `Vests`.
  - Accessories: `Locks`, `Lights`, `Headsets`, `Helmets`, `Pedals`, `Pumps`.
  - Components: subcategories containing `Frames` or in `Wheels`, `Saddles`.

## Sales Order Transformations

`publish_orders` applies:

- Join `store_sales_order_detail` with `store_sales_order_header` using `SalesOrderID`.
- Include all fields from SalesOrderDetail.
- Include all fields from SalesOrderHeader except `SalesOrderID`.
- Rename `Freight` to `TotalOrderFreight`.
- Calculate:

```text
TotalLineExtendedPrice = OrderQty * (UnitPrice - UnitPriceDiscount)
```

- Calculate business lead time excluding Saturdays and Sundays.

## Production Notes

For production, this design could be improved by:

- Replacing local Parquet with Delta tables.
- Using Unity Catalog or a managed metastore.
- Adding CI/CD automated tests.
- Setting `FAIL_ON_CRITICAL = True`.
- Adding alerting and data observability.
- Using a business calendar table to handle holidays.
- Supporting incremental loads with Delta Merge.
