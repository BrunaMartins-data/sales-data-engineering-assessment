# Databricks / PySpark solution for Interview Case Study
# Author: Bruna Martins
#
# Expected input files:
# - sales-order-detail.csv
# - sales-order-header.csv
# - products.csv
#
# Tables created:
# raw_sales_order_detail, raw_sales_order_header, raw_products
# store_sales_order_detail, store_sales_order_header, store_products
# publish_product, publish_orders

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
)

# -----------------------------
# Parameters
# -----------------------------
# In Databricks, update these paths or create widgets.
sales_order_detail_path = "/FileStore/tables/sales-order-detail.csv"
sales_order_header_path = "/FileStore/tables/sales-order-header.csv"
products_path = "/FileStore/tables/products.csv"

database_name = "default"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")


# -----------------------------
# Helpers
# -----------------------------
def read_csv_raw(path: str):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(path)
    )


def save_table(df, table_name: str, mode: str = "overwrite"):
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def parse_mixed_date(col_name: str):
    """
    Handles both yyyy-MM-dd and yyyy-MM values.
    When the source has yyyy-MM, it is converted to the first day of the month.
    """
    return (
        F.when(F.length(F.col(col_name)) == 7, F.to_date(F.concat(F.col(col_name), F.lit("-01"))))
         .otherwise(F.to_date(F.col(col_name)))
    )


# -----------------------------
# 1) Data Loading - raw_ tables
# -----------------------------
raw_sales_order_detail = read_csv_raw(sales_order_detail_path)
raw_sales_order_header = read_csv_raw(sales_order_header_path)
raw_products = read_csv_raw(products_path)

save_table(raw_sales_order_detail, "raw_sales_order_detail")
save_table(raw_sales_order_header, "raw_sales_order_header")
save_table(raw_products, "raw_products")


# -----------------------------
# 2) Data Review and Storage - store_ tables
# -----------------------------
store_sales_order_detail = (
    raw_sales_order_detail
    .select(
        F.col("SalesOrderID").cast(LongType()).alias("SalesOrderID"),
        F.col("SalesOrderDetailID").cast(LongType()).alias("SalesOrderDetailID"),
        F.col("OrderQty").cast(IntegerType()).alias("OrderQty"),
        F.col("ProductID").cast(LongType()).alias("ProductID"),
        F.col("UnitPrice").cast(DecimalType(18, 4)).alias("UnitPrice"),
        F.col("UnitPriceDiscount").cast(DecimalType(18, 4)).alias("UnitPriceDiscount"),
    )
)

store_sales_order_header = (
    raw_sales_order_header
    .select(
        F.col("SalesOrderID").cast(LongType()).alias("SalesOrderID"),
        parse_mixed_date("OrderDate").alias("OrderDate"),
        parse_mixed_date("ShipDate").alias("ShipDate"),
        F.col("OnlineOrderFlag").cast(BooleanType()).alias("OnlineOrderFlag"),
        F.col("AccountNumber").cast(StringType()).alias("AccountNumber"),
        F.col("CustomerID").cast(LongType()).alias("CustomerID"),
        F.col("SalesPersonID").cast(LongType()).alias("SalesPersonID"),
        F.col("Freight").cast(DecimalType(18, 4)).alias("Freight"),
    )
)

store_products_typed = (
    raw_products
    .select(
        F.col("ProductID").cast(LongType()).alias("ProductID"),
        F.col("ProductDesc").cast(StringType()).alias("ProductDesc"),
        F.col("ProductNumber").cast(StringType()).alias("ProductNumber"),
        F.col("MakeFlag").cast(BooleanType()).alias("MakeFlag"),
        F.col("Color").cast(StringType()).alias("Color"),
        F.col("SafetyStockLevel").cast(IntegerType()).alias("SafetyStockLevel"),
        F.col("ReorderPoint").cast(IntegerType()).alias("ReorderPoint"),
        F.col("StandardCost").cast(DecimalType(18, 4)).alias("StandardCost"),
        F.col("ListPrice").cast(DecimalType(18, 4)).alias("ListPrice"),
        F.col("Size").cast(StringType()).alias("Size"),
        F.col("SizeUnitMeasureCode").cast(StringType()).alias("SizeUnitMeasureCode"),
        F.col("Weight").cast(DecimalType(18, 4)).alias("Weight"),
        F.col("WeightUnitMeasureCode").cast(StringType()).alias("WeightUnitMeasureCode"),
        F.col("ProductCategoryName").cast(StringType()).alias("ProductCategoryName"),
        F.col("ProductSubCategoryName").cast(StringType()).alias("ProductSubCategoryName"),
    )
)

# ProductID appears duplicated in the product file.
# To prevent duplicated facts/revenue after joins, keep one trusted product row per ProductID,
# prioritizing rows where ProductCategoryName is already available.
product_dedup_window = (
    Window
    .partitionBy("ProductID")
    .orderBy(
        F.when(F.col("ProductCategoryName").isNotNull(), F.lit(1)).otherwise(F.lit(0)).desc(),
        F.col("ProductSubCategoryName").asc()
    )
)

store_products = (
    store_products_typed
    .withColumn("rn", F.row_number().over(product_dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

save_table(store_sales_order_detail, "store_sales_order_detail")
save_table(store_sales_order_header, "store_sales_order_header")
save_table(store_products, "store_products")


# -----------------------------
# 3) Product Master Transformations - publish_product
# -----------------------------
clothing_subcategories = ["Gloves", "Shorts", "Socks", "Tights", "Vests"]
accessories_subcategories = ["Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"]
components_subcategories = ["Wheels", "Saddles"]

publish_product = (
    store_products
    .withColumn("Color", F.coalesce(F.col("Color"), F.lit("N/A")))
    .withColumn(
        "ProductCategoryName",
        F.when(
            F.col("ProductCategoryName").isNull()
            & F.col("ProductSubCategoryName").isin(clothing_subcategories),
            F.lit("Clothing")
        )
        .when(
            F.col("ProductCategoryName").isNull()
            & F.col("ProductSubCategoryName").isin(accessories_subcategories),
            F.lit("Accessories")
        )
        .when(
            F.col("ProductCategoryName").isNull()
            & (
                F.col("ProductSubCategoryName").contains("Frames")
                | F.col("ProductSubCategoryName").isin(components_subcategories)
            ),
            F.lit("Components")
        )
        .otherwise(F.col("ProductCategoryName"))
    )
)

save_table(publish_product, "publish_product")


# -----------------------------
# 4) Sales Order Transformations - publish_orders
# -----------------------------
header_with_lead_time = (
    store_sales_order_header
    .withColumn(
        "LeadTimeInBusinessDays",
        F.when(
            F.col("ShipDate") > F.col("OrderDate"),
            F.size(
                F.expr(
                    """
                    filter(
                        sequence(OrderDate, date_sub(ShipDate, 1)),
                        x -> dayofweek(x) not in (1, 7)
                    )
                    """
                )
            )
        ).otherwise(F.lit(0))
    )
)

publish_orders = (
    store_sales_order_detail.alias("d")
    .join(header_with_lead_time.alias("h"), on="SalesOrderID", how="inner")
    .select(
        F.col("d.SalesOrderID"),
        F.col("d.SalesOrderDetailID"),
        F.col("d.OrderQty"),
        F.col("d.ProductID"),
        F.col("d.UnitPrice"),
        F.col("d.UnitPriceDiscount"),
        (F.col("d.OrderQty") * (F.col("d.UnitPrice") - F.col("d.UnitPriceDiscount"))).alias("TotalLineExtendedPrice"),
        F.col("h.OrderDate"),
        F.col("h.ShipDate"),
        F.col("h.OnlineOrderFlag"),
        F.col("h.AccountNumber"),
        F.col("h.CustomerID"),
        F.col("h.SalesPersonID"),
        F.col("h.Freight").alias("TotalOrderFreight"),
        F.col("h.LeadTimeInBusinessDays"),
    )
)

save_table(publish_orders, "publish_orders")


# -----------------------------
# 5) Analysis Questions
# -----------------------------
orders_with_product = (
    spark.table("publish_orders").alias("o")
    .join(
        spark.table("publish_product").select("ProductID", "Color", "ProductCategoryName").alias("p"),
        on="ProductID",
        how="left"
    )
)

# Q1: Which color generated the highest revenue each year?
revenue_by_color_year = (
    orders_with_product
    .groupBy(F.year("OrderDate").alias("Year"), "Color")
    .agg(F.sum("TotalLineExtendedPrice").alias("Revenue"))
)

rank_color_year = Window.partitionBy("Year").orderBy(F.col("Revenue").desc())

answer_highest_revenue_color_by_year = (
    revenue_by_color_year
    .withColumn("rn", F.row_number().over(rank_color_year))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .orderBy("Year")
)

answer_highest_revenue_color_by_year.show(truncate=False)

# Q2: What is the average LeadTimeInBusinessDays by ProductCategoryName?
answer_avg_lead_time_by_category = (
    orders_with_product
    .groupBy("ProductCategoryName")
    .agg(F.avg("LeadTimeInBusinessDays").alias("AvgLeadTimeInBusinessDays"))
    .orderBy(F.col("AvgLeadTimeInBusinessDays").desc())
)

answer_avg_lead_time_by_category.show(truncate=False)


# -----------------------------
# 6) Optional data quality checks
# -----------------------------
# Primary keys:
# - store_sales_order_detail: SalesOrderDetailID
# - store_sales_order_header: SalesOrderID
# - store_products / publish_product: ProductID
#
# Foreign keys:
# - store_sales_order_detail.SalesOrderID -> store_sales_order_header.SalesOrderID
# - store_sales_order_detail.ProductID -> store_products.ProductID

pk_checks = {
    "detail_rows": store_sales_order_detail.count(),
    "detail_distinct_pk": store_sales_order_detail.select("SalesOrderDetailID").distinct().count(),
    "header_rows": store_sales_order_header.count(),
    "header_distinct_pk": store_sales_order_header.select("SalesOrderID").distinct().count(),
    "product_rows": store_products.count(),
    "product_distinct_pk": store_products.select("ProductID").distinct().count(),
}

print(pk_checks)

fk_orders_missing_header = (
    store_sales_order_detail.select("SalesOrderID").distinct()
    .join(store_sales_order_header.select("SalesOrderID").distinct(), on="SalesOrderID", how="left_anti")
    .count()
)

fk_orders_missing_product = (
    store_sales_order_detail.select("ProductID").distinct()
    .join(store_products.select("ProductID").distinct(), on="ProductID", how="left_anti")
    .count()
)

print({
    "fk_orders_missing_header": fk_orders_missing_header,
    "fk_orders_missing_product": fk_orders_missing_product,
})
