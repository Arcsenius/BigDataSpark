import base64
import json
import os
import urllib.parse
import urllib.request
from datetime import date, datetime
from decimal import Decimal
from urllib.error import HTTPError

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "pet_sales")
POSTGRES_USER = os.getenv("POSTGRES_USER", "spark")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "spark")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "pet_sales_reports")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "spark")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "spark")
INPUT_PATH = os.getenv("INPUT_PATH", "/app/исходные данные/*.csv")


def postgres_properties():
    return {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def postgres_url():
    return f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def write_postgres(df, table):
    df.write.jdbc(
        url=postgres_url(),
        table=table,
        mode="overwrite",
        properties=postgres_properties(),
    )


def read_postgres(spark, table):
    return spark.read.jdbc(
        url=postgres_url(),
        table=table,
        properties=postgres_properties(),
    )


def clickhouse_request(query, data=None):
    params = urllib.parse.urlencode({"database": CLICKHOUSE_DB})
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?{params}"
    body = query.encode("utf-8") if data is None else query.encode("utf-8") + data
    request = urllib.request.Request(url, data=body, method="POST")
    token = base64.b64encode(f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}".encode("utf-8")).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read().decode("utf-8")
    except HTTPError as error:
        detail = error.read().decode("utf-8")
        raise RuntimeError(detail) from error


def json_value(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def write_clickhouse_table(df, table, columns_sql, order_by):
    clickhouse_request(f"DROP TABLE IF EXISTS {table}")
    clickhouse_request(f"CREATE TABLE {table} ({columns_sql}) ENGINE = MergeTree ORDER BY {order_by}")
    rows = []
    for row in df.collect():
        rows.append(json.dumps({k: json_value(v) for k, v in row.asDict().items()}, ensure_ascii=False))
    if rows:
        payload = ("\n".join(rows) + "\n").encode("utf-8")
        clickhouse_request(f"INSERT INTO {table} FORMAT JSONEachRow\n", payload)


def cast_source(raw):
    return (
        raw.withColumn("source_file", F.input_file_name())
        .withColumn("source_row_number", F.row_number().over(Window.orderBy(F.input_file_name(), F.col("id").cast("int"))))
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("customer_age", F.col("customer_age").cast("int"))
        .withColumn("product_price", F.col("product_price").cast("decimal(12,2)"))
        .withColumn("product_quantity", F.col("product_quantity").cast("int"))
        .withColumn("sale_date", F.to_date("sale_date", "M/d/yyyy"))
        .withColumn("sale_customer_id", F.col("sale_customer_id").cast("long"))
        .withColumn("sale_seller_id", F.col("sale_seller_id").cast("long"))
        .withColumn("sale_product_id", F.col("sale_product_id").cast("long"))
        .withColumn("sale_quantity", F.col("sale_quantity").cast("int"))
        .withColumn("sale_total_price", F.col("sale_total_price").cast("decimal(12,2)"))
        .withColumn("product_weight", F.col("product_weight").cast("decimal(12,2)"))
        .withColumn("product_rating", F.col("product_rating").cast("decimal(3,1)"))
        .withColumn("product_reviews", F.col("product_reviews").cast("int"))
        .withColumn("product_release_date", F.to_date("product_release_date", "M/d/yyyy"))
        .withColumn("product_expiry_date", F.to_date("product_expiry_date", "M/d/yyyy"))
    )


def build_star(raw):
    customer_window = Window.partitionBy("sale_customer_id").orderBy("source_row_number")
    seller_window = Window.partitionBy("sale_seller_id").orderBy("source_row_number")
    product_window = Window.partitionBy("sale_product_id").orderBy("source_row_number")
    store_window = Window.partitionBy("store_name", "store_city", "store_country").orderBy("source_row_number")
    supplier_window = Window.partitionBy("supplier_name", "supplier_city", "supplier_country").orderBy("source_row_number")
    dim_customer = (
        raw.withColumn("rn", F.row_number().over(customer_window))
        .filter("rn = 1")
        .select(
            F.col("sale_customer_id").alias("customer_id"),
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
        )
    )

    dim_seller = (
        raw.withColumn("rn", F.row_number().over(seller_window))
        .filter("rn = 1")
        .select(
            F.col("sale_seller_id").alias("seller_id"),
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code",
        )
    )

    dim_supplier_base = (
        raw.withColumn("rn", F.row_number().over(supplier_window))
        .filter("rn = 1")
        .select(
            F.dense_rank().over(Window.orderBy("supplier_name", "supplier_city", "supplier_country")).alias("supplier_id"),
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
    )

    dim_product = (
        raw.join(dim_supplier_base, ["supplier_name", "supplier_city", "supplier_country"], "left")
        .withColumn("rn", F.row_number().over(product_window))
        .filter("rn = 1")
        .select(
            F.col("sale_product_id").alias("product_id"),
            "supplier_id",
            "product_name",
            "product_category",
            "product_price",
            "product_quantity",
            "pet_category",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
        )
    )

    dim_store = (
        raw.withColumn("rn", F.row_number().over(store_window))
        .filter("rn = 1")
        .select(
            F.dense_rank().over(Window.orderBy("store_name", "store_city", "store_country")).alias("store_id"),
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        )
    )

    dim_date = (
        raw.select("sale_date")
        .where(F.col("sale_date").isNotNull())
        .distinct()
        .select(
            F.dense_rank().over(Window.orderBy("sale_date")).alias("date_id"),
            "sale_date",
            F.year("sale_date").alias("sale_year"),
            F.month("sale_date").alias("sale_month"),
            F.quarter("sale_date").alias("sale_quarter"),
            F.date_format("sale_date", "MMMM").alias("sale_month_name"),
        )
    )

    fact_sales = (
        raw.join(dim_store, ["store_name", "store_city", "store_country"], "left")
        .join(dim_date, ["sale_date"], "left")
        .select(
            F.col("source_row_number").alias("sale_id"),
            "date_id",
            F.col("sale_customer_id").alias("customer_id"),
            F.col("sale_seller_id").alias("seller_id"),
            F.col("sale_product_id").alias("product_id"),
            "store_id",
            "sale_quantity",
            "sale_total_price",
            "source_file",
        )
    )

    return {
        "dim_customer": dim_customer,
        "dim_seller": dim_seller,
        "dim_supplier": dim_supplier_base,
        "dim_product": dim_product,
        "dim_store": dim_store,
        "dim_date": dim_date,
        "fact_sales": fact_sales,
    }


def ranked(df, partitionless_window):
    return df.withColumn("rank", F.row_number().over(partitionless_window))


def build_reports(star):
    fact = star["fact_sales"]
    product = star["dim_product"]
    customer = star["dim_customer"]
    store = star["dim_store"]
    supplier = star["dim_supplier"]
    date_dim = star["dim_date"]

    sales_product = fact.join(product, "product_id")
    product_base = sales_product.groupBy("product_id", "product_name", "product_category").agg(
        F.sum("sale_quantity").alias("total_quantity"),
        F.sum("sale_total_price").alias("total_revenue"),
        F.avg("product_rating").alias("avg_rating"),
        F.sum("product_reviews").alias("total_reviews"),
    )
    top_products = ranked(product_base.orderBy(F.desc("total_quantity")), Window.orderBy(F.desc("total_quantity"))).limit(10)
    category_revenue = (
        product_base.groupBy("product_category")
        .agg(F.sum("total_revenue").alias("total_revenue"), F.sum("total_quantity").alias("total_quantity"))
        .withColumn("product_id", F.lit(None).cast("long"))
        .withColumn("product_name", F.lit(None).cast("string"))
        .withColumn("avg_rating", F.lit(None).cast("double"))
        .withColumn("total_reviews", F.lit(None).cast("long"))
        .withColumn("rank", F.lit(None).cast("int"))
        .select(top_products.columns)
    )
    product_rating = (
        product_base.withColumn("rank", F.lit(None).cast("int"))
        .select(top_products.columns)
    )
    report_products = (
        top_products.withColumn("metric", F.lit("top_10_products"))
        .unionByName(category_revenue.withColumn("metric", F.lit("category_revenue")))
        .unionByName(product_rating.withColumn("metric", F.lit("product_rating_reviews")))
        .select("metric", "rank", "product_id", "product_name", "product_category", "total_quantity", "total_revenue", "avg_rating", "total_reviews")
    )

    customer_base = fact.join(customer, "customer_id").groupBy(
        "customer_id", "customer_first_name", "customer_last_name", "customer_email", "customer_country"
    ).agg(
        F.count("*").alias("orders_count"),
        F.sum("sale_total_price").alias("total_spent"),
        F.avg("sale_total_price").alias("avg_check"),
    )
    top_customers = ranked(customer_base.orderBy(F.desc("total_spent")), Window.orderBy(F.desc("total_spent"))).limit(10)
    country_distribution = (
        customer.groupBy("customer_country")
        .agg(F.countDistinct("customer_id").alias("customers_count"))
        .withColumn("customer_id", F.lit(None).cast("long"))
        .withColumn("customer_first_name", F.lit(None).cast("string"))
        .withColumn("customer_last_name", F.lit(None).cast("string"))
        .withColumn("customer_email", F.lit(None).cast("string"))
        .withColumn("orders_count", F.lit(None).cast("long"))
        .withColumn("total_spent", F.lit(None).cast("decimal(20,2)"))
        .withColumn("avg_check", F.lit(None).cast("double"))
        .withColumn("rank", F.lit(None).cast("int"))
        .select("customer_id", "customer_first_name", "customer_last_name", "customer_email", "customer_country", "orders_count", "total_spent", "avg_check", "rank", "customers_count")
    )
    customer_checks = customer_base.withColumn("rank", F.lit(None).cast("int")).withColumn("customers_count", F.lit(None).cast("long"))
    report_customers = (
        top_customers.withColumn("customers_count", F.lit(None).cast("long")).withColumn("metric", F.lit("top_10_customers"))
        .unionByName(country_distribution.withColumn("metric", F.lit("country_distribution")))
        .unionByName(customer_checks.withColumn("metric", F.lit("customer_avg_check")))
        .select("metric", "rank", "customer_id", "customer_first_name", "customer_last_name", "customer_email", "customer_country", "orders_count", "total_spent", "avg_check", "customers_count")
    )

    sales_time = fact.join(date_dim, "date_id")
    report_time = (
        sales_time.groupBy("sale_year", "sale_month")
        .agg(
            F.count("*").alias("orders_count"),
            F.sum("sale_quantity").alias("total_quantity"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("sale_total_price").alias("avg_order_value"),
        )
        .withColumn("period", F.format_string("%04d-%02d", F.col("sale_year"), F.col("sale_month")))
        .withColumn("previous_period_revenue", F.lag("total_revenue").over(Window.orderBy("sale_year", "sale_month")))
        .withColumn("revenue_delta", F.col("total_revenue") - F.col("previous_period_revenue"))
        .select("period", "sale_year", "sale_month", "orders_count", "total_quantity", "total_revenue", "avg_order_value", "previous_period_revenue", "revenue_delta")
    )

    store_base = fact.join(store, "store_id").groupBy("store_id", "store_name", "store_city", "store_country").agg(
        F.count("*").alias("orders_count"),
        F.sum("sale_quantity").alias("total_quantity"),
        F.sum("sale_total_price").alias("total_revenue"),
        F.avg("sale_total_price").alias("avg_check"),
    )
    top_stores = ranked(store_base.orderBy(F.desc("total_revenue")), Window.orderBy(F.desc("total_revenue"))).limit(5)
    city_country_sales = (
        store_base.groupBy("store_city", "store_country")
        .agg(F.sum("orders_count").alias("orders_count"), F.sum("total_quantity").alias("total_quantity"), F.sum("total_revenue").alias("total_revenue"))
        .withColumn("store_id", F.lit(None).cast("long"))
        .withColumn("store_name", F.lit(None).cast("string"))
        .withColumn("avg_check", F.lit(None).cast("double"))
        .withColumn("rank", F.lit(None).cast("int"))
        .select(top_stores.columns)
    )
    store_checks = store_base.withColumn("rank", F.lit(None).cast("int"))
    report_stores = (
        top_stores.withColumn("metric", F.lit("top_5_stores"))
        .unionByName(city_country_sales.withColumn("metric", F.lit("city_country_sales")))
        .unionByName(store_checks.withColumn("metric", F.lit("store_avg_check")))
        .select("metric", "rank", "store_id", "store_name", "store_city", "store_country", "orders_count", "total_quantity", "total_revenue", "avg_check")
    )

    supplier_sales = fact.join(product, "product_id").join(supplier, "supplier_id")
    supplier_base = supplier_sales.groupBy("supplier_id", "supplier_name", "supplier_country").agg(
        F.count("*").alias("orders_count"),
        F.sum("sale_quantity").alias("total_quantity"),
        F.sum("sale_total_price").alias("total_revenue"),
        F.avg("product_price").alias("avg_product_price"),
    )
    top_suppliers = ranked(supplier_base.orderBy(F.desc("total_revenue")), Window.orderBy(F.desc("total_revenue"))).limit(5)
    supplier_country_sales = (
        supplier_base.groupBy("supplier_country")
        .agg(F.sum("orders_count").alias("orders_count"), F.sum("total_quantity").alias("total_quantity"), F.sum("total_revenue").alias("total_revenue"))
        .withColumn("supplier_id", F.lit(None).cast("long"))
        .withColumn("supplier_name", F.lit(None).cast("string"))
        .withColumn("avg_product_price", F.lit(None).cast("double"))
        .withColumn("rank", F.lit(None).cast("int"))
        .select(top_suppliers.columns)
    )
    supplier_prices = supplier_base.withColumn("rank", F.lit(None).cast("int"))
    report_suppliers = (
        top_suppliers.withColumn("metric", F.lit("top_5_suppliers"))
        .unionByName(supplier_prices.withColumn("metric", F.lit("supplier_avg_price")))
        .unionByName(supplier_country_sales.withColumn("metric", F.lit("supplier_country_sales")))
        .select("metric", "rank", "supplier_id", "supplier_name", "supplier_country", "orders_count", "total_quantity", "total_revenue", "avg_product_price")
    )

    quality_base = product_base.select("product_id", "product_name", "product_category", "total_quantity", "total_revenue", "avg_rating", "total_reviews")
    highest = ranked(quality_base.orderBy(F.desc("avg_rating")), Window.orderBy(F.desc("avg_rating"))).limit(10).withColumn("metric", F.lit("highest_rating"))
    lowest = ranked(quality_base.orderBy(F.asc("avg_rating")), Window.orderBy(F.asc("avg_rating"))).limit(10).withColumn("metric", F.lit("lowest_rating"))
    most_reviews = ranked(quality_base.orderBy(F.desc("total_reviews")), Window.orderBy(F.desc("total_reviews"))).limit(10).withColumn("metric", F.lit("most_reviews"))
    correlation_value = sales_product.select(F.corr(F.col("product_rating").cast("double"), F.col("sale_quantity").cast("double")).alias("rating_sales_correlation")).first()[0]
    correlation = spark_singleton().createDataFrame(
        [("rating_sales_correlation", None, None, None, None, None, None, None, None, correlation_value)],
        "metric string, rank int, product_id long, product_name string, product_category string, total_quantity long, total_revenue decimal(20,2), avg_rating double, total_reviews long, rating_sales_correlation double",
    )
    report_quality = (
        highest.unionByName(lowest).unionByName(most_reviews)
        .withColumn("rating_sales_correlation", F.lit(None).cast("double"))
        .select("metric", "rank", "product_id", "product_name", "product_category", "total_quantity", "total_revenue", "avg_rating", "total_reviews", "rating_sales_correlation")
        .unionByName(correlation)
    )

    return {
        "report_product_sales": report_products,
        "report_customer_sales": report_customers,
        "report_time_sales": report_time,
        "report_store_sales": report_stores,
        "report_supplier_sales": report_suppliers,
        "report_product_quality": report_quality,
    }


def spark_singleton():
    return SparkSession.getActiveSession()


def main():
    spark = (
        SparkSession.builder.appName("BigDataSparkLab")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    raw = spark.read.option("header", True).option("multiLine", True).option("escape", "\"").csv(INPUT_PATH)
    typed_raw = cast_source(raw)
    write_postgres(typed_raw, "mock_data")

    star = build_star(typed_raw)
    for table, df in star.items():
        write_postgres(df, table)

    persisted_star = {table: read_postgres(spark, table) for table in star}
    reports = build_reports(persisted_star)

    write_clickhouse_table(
        reports["report_product_sales"],
        "report_product_sales",
        "metric String, rank Nullable(UInt32), product_id Nullable(UInt64), product_name Nullable(String), product_category Nullable(String), total_quantity Nullable(UInt64), total_revenue Nullable(Float64), avg_rating Nullable(Float64), total_reviews Nullable(UInt64)",
        "tuple()",
    )
    write_clickhouse_table(
        reports["report_customer_sales"],
        "report_customer_sales",
        "metric String, rank Nullable(UInt32), customer_id Nullable(UInt64), customer_first_name Nullable(String), customer_last_name Nullable(String), customer_email Nullable(String), customer_country Nullable(String), orders_count Nullable(UInt64), total_spent Nullable(Float64), avg_check Nullable(Float64), customers_count Nullable(UInt64)",
        "tuple()",
    )
    write_clickhouse_table(
        reports["report_time_sales"],
        "report_time_sales",
        "period String, sale_year UInt16, sale_month UInt8, orders_count UInt64, total_quantity UInt64, total_revenue Float64, avg_order_value Float64, previous_period_revenue Nullable(Float64), revenue_delta Nullable(Float64)",
        "tuple()",
    )
    write_clickhouse_table(
        reports["report_store_sales"],
        "report_store_sales",
        "metric String, rank Nullable(UInt32), store_id Nullable(UInt64), store_name Nullable(String), store_city Nullable(String), store_country Nullable(String), orders_count Nullable(UInt64), total_quantity Nullable(UInt64), total_revenue Nullable(Float64), avg_check Nullable(Float64)",
        "tuple()",
    )
    write_clickhouse_table(
        reports["report_supplier_sales"],
        "report_supplier_sales",
        "metric String, rank Nullable(UInt32), supplier_id Nullable(UInt64), supplier_name Nullable(String), supplier_country Nullable(String), orders_count Nullable(UInt64), total_quantity Nullable(UInt64), total_revenue Nullable(Float64), avg_product_price Nullable(Float64)",
        "tuple()",
    )
    write_clickhouse_table(
        reports["report_product_quality"],
        "report_product_quality",
        "metric String, rank Nullable(UInt32), product_id Nullable(UInt64), product_name Nullable(String), product_category Nullable(String), total_quantity Nullable(UInt64), total_revenue Nullable(Float64), avg_rating Nullable(Float64), total_reviews Nullable(UInt64), rating_sales_correlation Nullable(Float64)",
        "tuple()",
    )

    spark.stop()


if __name__ == "__main__":
    main()
