# Databricks notebook source

from pyspark.sql import functions as F

# Read saved Silver Delta tables
accounts = spark.table("dbw_fraud_dev.customer360.silver_accounts")
opportunities = spark.table("dbw_fraud_dev.customer360.silver_opportunities")
sales = spark.table("dbw_fraud_dev.customer360.silver_sales")

# Aggregate opportunities at customer level
opp_summary = (
    opportunities.groupBy("account_id")
    .agg(
        F.count("opportunity_id").alias("opportunity_count"),
        F.sum("amount").alias("total_opportunity_amount"),
        F.sum(
            F.when(F.lower(F.trim(F.col("stage"))) == "closed won",
                   F.col("amount"))
             .otherwise(F.lit(0))
        ).alias("closed_won_amount")
    )
)

# Aggregate SAP revenue at customer level
sales_summary = (
    sales.groupBy("account_id")
    .agg(
        F.sum("revenue").alias("total_sales_revenue")
    )
)

# Build Gold Customer 360 view
gold_customer360 = (
    accounts
    .join(opp_summary, "account_id", "left")
    .join(sales_summary, "account_id", "left")
    .fillna({
        "opportunity_count": 0,
        "total_opportunity_amount": 0,
        "closed_won_amount": 0,
        "total_sales_revenue": 0
    })
)

# Save Gold Delta table
gold_customer360.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_fraud_dev.customer360.gold_customer360")

display(gold_customer360)

print("Gold Customer 360 Records:", gold_customer360.count())
print("Gold Delta table saved successfully!")


# COMMAND ----------


# Create Customer 360 Gold KPI Delta Table

spark.sql("""
CREATE OR REPLACE TABLE
dbw_fraud_dev.customer360.gold_customer360_kpis
USING DELTA
AS
SELECT
    COUNT(DISTINCT account_id) AS total_customers,
    SUM(opportunity_count) AS total_opportunities,
    SUM(total_opportunity_amount) AS total_pipeline_amount,
    SUM(closed_won_amount) AS total_closed_won_amount,
    SUM(total_sales_revenue) AS total_sales_revenue
FROM dbw_fraud_dev.customer360.gold_customer360
""")

# Display KPI results
display(spark.sql("""
SELECT *
FROM dbw_fraud_dev.customer360.gold_customer360_kpis
"""))

print("Customer 360 KPI Delta table saved successfully!")
