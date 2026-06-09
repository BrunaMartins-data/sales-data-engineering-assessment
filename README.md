# sales-data-engineering-assessment
PySpark data engineering assessment for loading raw sales and product data, applying data type review, creating transformed store tables, publishing analytical datasets, and answering business questions.

# Interview Case Study - Data Loading, Transformations and Analysis

## Scope

This project loads three CSV files using PySpark, creates raw and typed/stored tables, publishes transformed product and order datasets, and answers the two requested analysis questions.

## Tables

### Raw tables

- `raw_sales_order_detail`
- `raw_sales_order_header`
- `raw_products`

### Store tables

- `store_sales_order_detail`
- `store_sales_order_header`
- `store_products`

### Publish tables

- `publish_product`
- `publish_orders`

## Data model review

### Primary keys

- `store_sales_order_detail`: `SalesOrderDetailID`
- `store_sales_order_header`: `SalesOrderID`
- `store_products` / `publish_product`: `ProductID`

### Foreign keys

- `store_sales_order_detail.SalesOrderID` → `store_sales_order_header.SalesOrderID`
- `store_sales_order_detail.ProductID` → `store_products.ProductID`

## Data quality observations

- `SalesOrderDetailID` is unique in sales order detail.
- `SalesOrderID` is unique in sales order header.
- `ProductID` has duplicates in the raw product file. The solution deduplicates products by keeping one row per `ProductID`, prioritizing records with `ProductCategoryName` already populated. This avoids duplicated revenue after joining orders to products.
- `OrderDate` has mixed formats: mostly `yyyy-MM-dd`, with a few `yyyy-MM`. The solution converts `yyyy-MM` to the first day of the month.

## Business day logic

`LeadTimeInBusinessDays` is calculated as the number of weekdays from `OrderDate` up to the day before `ShipDate`, excluding Saturdays and Sundays. This follows `datediff`-style semantics where the end date is not counted.

## Local validation results on the provided files

### Highest revenue color by year

| Year | Color | Revenue |
|---:|---|---:|
| 2021 | Red | 6,019,614.08 |
| 2022 | Black | 14,005,238.36 |
| 2023 | Black | 15,047,692.77 |
| 2024 | Yellow | 6,368,158.11 |

### Average LeadTimeInBusinessDays by ProductCategoryName

| ProductCategoryName | AvgLeadTimeInBusinessDays |
|---|---:|
| NULL / Uncategorized | 5.0108 |
| Accessories | 5.0065 |
| Clothing | 5.0051 |
| Bikes | 5.0049 |
| Components | 5.0033 |

## How to run

1. Upload the three CSV files to Databricks.
2. Update the file paths at the top of `assessment_solution.py`.
3. Run the script in a Databricks notebook or as a job.
4. Validate the generated tables and outputs.
