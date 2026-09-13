from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    year,
    datediff,
    current_date,
    when
)
from delta import configure_spark_with_delta_pip


# Create Spark session with Delta support
builder = (
    SparkSession.builder
    .appName("CustomerGoldLayer")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Paths
silver_path = "data/silver_delta/customers"
gold_path = "data/gold/customers"


# Read Silver Delta table
print("Reading Silver Delta table...")

df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

print("Silver record count:", df.count())


# Create business-friendly Gold dataset
gold_df = (
    df
    .withColumn(
        "full_name",
        concat_ws(" ", col("first_name"), col("last_name"))
    )
    .withColumn(
        "signup_year",
        year(col("signup_date"))
    )
    .withColumn(
        "customer_age_in_days",
        datediff(current_date(), col("signup_date"))
    )
    .withColumn(
        "is_active",
        when(col("status") == "ACTIVE", True).otherwise(False)
    )
    .select(
        "customer_id",
        "full_name",
        "email",
        "city",
        "state",
        "country",
        "signup_date",
        "signup_year",
        "customer_age_in_days",
        "status",
        "is_active",
        "updated_at"
    )
)


print("Gold customer data:")
gold_df.orderBy("customer_id").show(truncate=False)


# Write Gold as Delta
print("Writing Gold Delta table...")

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .save(gold_path)
)

print("Gold customer table created successfully.")


# Business aggregations
print("\nCustomers by city:")

(
    gold_df
    .groupBy("city")
    .count()
    .orderBy(col("count").desc())
    .show()
)


print("Customers by state:")

(
    gold_df
    .groupBy("state")
    .count()
    .orderBy(col("count").desc())
    .show()
)


print("Active vs Inactive customers:")

(
    gold_df
    .groupBy("status")
    .count()
    .orderBy(col("count").desc())
    .show()
)


spark.stop()