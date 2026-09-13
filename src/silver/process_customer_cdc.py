from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip


# Create Spark session with Delta support
builder = (
    SparkSession.builder
    .appName("CustomerCDCProcessing")
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
delta_path = "data/silver_delta/customers"
cdc_path = "data/raw/customers_cdc_batch.csv"


# Read CDC batch
cdc_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(cdc_path)
)

print("Incoming CDC records:")
cdc_df.show(truncate=False)

print("Incoming CDC count:", cdc_df.count())


# Clean and standardize
cdc_clean = (
    cdc_df
    .withColumn("operation", upper(trim(col("operation"))))
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", trim(col("email")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("country", trim(col("country")))
    .withColumn("status", upper(trim(col("status"))))
)


# Data quality validation
# DELETE records don't require email.
cdc_valid = cdc_clean.filter(
    col("customer_id").isNotNull()
    & col("operation").isin("I", "U", "D")
    & (
        (col("operation") == "D")
        | col("email").isNotNull()
    )
)

print("Valid CDC records:", cdc_valid.count())

cdc_valid.show(truncate=False)


# Keep latest CDC event for each customer
window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(col("updated_at").desc())
)

cdc_latest = (
    cdc_valid
    .withColumn("row_number", row_number().over(window_spec))
    .filter(col("row_number") == 1)
    .drop("row_number")
)

print("Latest CDC records after deduplication:")
cdc_latest.show(truncate=False)


# Load Delta table
delta_table = DeltaTable.forPath(
    spark,
    delta_path
)


# Perform CDC MERGE
print("Starting CDC MERGE...")

(
    delta_table.alias("target")
    .merge(
        cdc_latest.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedDelete(
        condition="source.operation = 'D'"
    )
    .whenMatchedUpdate(
        condition=(
            "source.operation = 'U' "
            "AND source.updated_at > target.updated_at"
        ),
        set={
            "first_name": "source.first_name",
            "last_name": "source.last_name",
            "email": "source.email",
            "city": "source.city",
            "state": "source.state",
            "country": "source.country",
            "signup_date": "source.signup_date",
            "status": "source.status",
            "updated_at": "source.updated_at"
        }
    )
    .whenNotMatchedInsert(
        condition="source.operation = 'I'",
        values={
            "customer_id": "source.customer_id",
            "first_name": "source.first_name",
            "last_name": "source.last_name",
            "email": "source.email",
            "city": "source.city",
            "state": "source.state",
            "country": "source.country",
            "signup_date": "source.signup_date",
            "status": "source.status",
            "updated_at": "source.updated_at"
        }
    )
    .execute()
)


print("CDC MERGE completed successfully.")


# Verify final Delta table
final_df = (
    spark.read
    .format("delta")
    .load(delta_path)
)

print("Final Delta record count:", final_df.count())

print("Final customer data:")
final_df.orderBy("customer_id").show(truncate=False)


spark.stop()