from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------
# Create Spark session with Delta support
# ---------------------------------------------------------

builder = (
    SparkSession.builder
    .appName("CustomerDataQuality")
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


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

silver_path = "data/silver_delta/customers"


print("=" * 55)
print("           CUSTOMER DATA QUALITY REPORT")
print("=" * 55)


# ---------------------------------------------------------
# Read Silver Delta table
# ---------------------------------------------------------

df = (
    spark.read
    .format("delta")
    .load(silver_path)
)

total_records = df.count()

print(f"\nTotal records              : {total_records}")


# ---------------------------------------------------------
# 1. Null customer_id
# ---------------------------------------------------------

null_customer_id = df.filter(
    col("customer_id").isNull()
).count()

print(f"Null customer IDs          : {null_customer_id}")


# ---------------------------------------------------------
# 2. Null email
# ---------------------------------------------------------

null_email = df.filter(
    col("email").isNull() |
    (trim(col("email")) == "")
).count()

print(f"Null emails                : {null_email}")


# ---------------------------------------------------------
# 3. Duplicate customer IDs
# ---------------------------------------------------------

duplicate_customer_ids = (
    df.groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(f"Duplicate customer IDs     : {duplicate_customer_ids}")


# ---------------------------------------------------------
# 4. Invalid status
# ---------------------------------------------------------

invalid_status = df.filter(
    ~upper(trim(col("status"))).isin(
        "ACTIVE",
        "INACTIVE"
    )
).count()

print(f"Invalid statuses           : {invalid_status}")


# ---------------------------------------------------------
# 5. Null signup date
# ---------------------------------------------------------

null_signup_date = df.filter(
    col("signup_date").isNull()
).count()

print(f"Null signup dates          : {null_signup_date}")


# ---------------------------------------------------------
# Overall result
# ---------------------------------------------------------

dq_failed = (
    null_customer_id
    + null_email
    + duplicate_customer_ids
    + invalid_status
    + null_signup_date
)

print("\n" + "=" * 55)

if dq_failed == 0:
    print("Overall Data Quality       : PASSED")
    print("=" * 55)
else:
    print("Overall Data Quality       : FAILED")
    print("=" * 55)
    spark.stop()
    raise SystemExit(1)

spark.stop()