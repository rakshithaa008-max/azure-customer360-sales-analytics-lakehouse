# Databricks notebook source
display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

display(spark.sql("SHOW EXTERNAL LOCATIONS"))

# COMMAND ----------


# Customer 360 - Read Salesforce Accounts from ADLS Bronze

accounts_path = (
    "abfss://bronze@stcust360demo26.dfs.core.windows.net/"
    "salesforce/accounts/salesforce_accounts.csv"
)

df_accounts = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(accounts_path)
)

display(df_accounts)
print("Total Accounts:", df_accounts.count())


# COMMAND ----------


# Read Salesforce Opportunities
opportunities_path = (
    "abfss://bronze@stcust360demo26.dfs.core.windows.net/"
    "salesforce/opportunities/salesforce_opportunities.csv"
)

df_opportunities = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(opportunities_path)
)

display(df_opportunities)
print("Total Opportunities:", df_opportunities.count())

# Read SAP BO Sales Report
sales_path = (
    "abfss://bronze@stcust360demo26.dfs.core.windows.net/"
    "sap_bo/sales_report/sap_bo_sales_report.csv"
)

df_sales = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(sales_path)
)

display(df_sales)
print("Total Sales Records:", df_sales.count())


# COMMAND ----------


from pyspark.sql import functions as F

# SILVER 1: Clean Salesforce Accounts
silver_accounts = (
    df_accounts
    .withColumn("account_id", F.trim("account_id"))
    .withColumn("account_name", F.trim("account_name"))
    .withColumn("city", F.trim("city"))
    .withColumn("industry", F.trim("industry"))
    .filter(
        F.col("account_id").isNotNull() &
        (F.col("account_id") != "")
    )
    .dropDuplicates(["account_id"])
)

# SILVER 2: Clean Salesforce Opportunities
silver_opportunities = (
    df_opportunities
    .withColumn("opportunity_id", F.trim("opportunity_id"))
    .withColumn("account_id", F.trim("account_id"))
    .withColumn("stage", F.trim("stage"))
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .filter(
        F.col("opportunity_id").isNotNull() &
        (F.col("opportunity_id") != "") &
        F.col("account_id").isNotNull() &
        (F.col("account_id") != "") &
        F.col("amount").isNotNull() &
        (F.col("amount") >= 0)
    )
    .dropDuplicates(["opportunity_id"])
)

# SILVER 3: Clean SAP Sales Records
silver_sales = (
    df_sales
    .withColumn("report_id", F.trim("report_id"))
    .withColumn("account_id", F.trim("account_id"))
    .withColumn("region", F.trim("region"))
    .withColumn("revenue", F.col("revenue").cast("decimal(18,2)"))
    .filter(
        F.col("report_id").isNotNull() &
        (F.col("report_id") != "") &
        F.col("account_id").isNotNull() &
        (F.col("account_id") != "") &
        F.col("revenue").isNotNull() &
        (F.col("revenue") >= 0)
    )
    .dropDuplicates(["report_id"])
)

print("Silver Accounts:", silver_accounts.count())
print("Silver Opportunities:", silver_opportunities.count())
print("Silver Sales:", silver_sales.count())

display(silver_accounts)


# COMMAND ----------


# STEP 1: Create separate Customer 360 schema
spark.sql("""
CREATE SCHEMA IF NOT EXISTS dbw_fraud_dev.customer360
""")

# STEP 2: Save Silver Accounts
silver_accounts.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_fraud_dev.customer360.silver_accounts")

# STEP 3: Save Silver Opportunities
silver_opportunities.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_fraud_dev.customer360.silver_opportunities")

# STEP 4: Save Silver Sales
silver_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_fraud_dev.customer360.silver_sales")

print("Silver Delta tables saved successfully!")

display(spark.sql("""
SHOW TABLES IN dbw_fraud_dev.customer360
"""))


# COMMAND ----------


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
