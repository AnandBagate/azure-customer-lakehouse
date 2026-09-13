from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip


# --------------------------------------------------
# 1. Create Spark Session with Delta Lake
# --------------------------------------------------

builder = (
    SparkSession.builder
    .appName("CustomerDeltaMerge")
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


# --------------------------------------------------
# 2. Define paths
# --------------------------------------------------

delta_path = "data/silver_delta/customers"
batch_path = "data/raw/customers_batch3.csv"


# --------------------------------------------------
# 3. Read incoming batch
# --------------------------------------------------

batch_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(batch_path)
)

print("Incoming batch count:", batch_df.count())

batch_df.show(truncate=False)


# --------------------------------------------------
# 4. Data Quality filtering
# --------------------------------------------------

batch_clean = batch_df.filter(
    col("customer_id").isNotNull()
    & col("email").isNotNull()
)


# --------------------------------------------------
# 5. Standardize data
# --------------------------------------------------

batch_clean = (
    batch_clean
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", trim(col("email")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("country", trim(col("country")))
    .withColumn("status", upper(trim(col("status"))))
)


# --------------------------------------------------
# 6. Deduplicate incoming batch
#    Keep latest record per customer_id
# --------------------------------------------------

window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(col("updated_at").desc())
)

batch_latest = (
    batch_clean
    .withColumn("row_number", row_number().over(window_spec))
    .filter(col("row_number") == 1)
    .drop("row_number")
)

print("Valid deduplicated records:", batch_latest.count())

batch_latest.show(truncate=False)


# --------------------------------------------------
# 7. Load Delta table
# --------------------------------------------------

delta_table = DeltaTable.forPath(
    spark,
    delta_path
)


# --------------------------------------------------
# 8. Perform Delta MERGE
# --------------------------------------------------

print("Starting Delta MERGE...")

(
    delta_table.alias("target")
    .merge(
        batch_latest.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedUpdate(
        condition="source.updated_at > target.updated_at",
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


print("Delta MERGE completed successfully.")


# --------------------------------------------------
# 9. Verify final Delta table
# --------------------------------------------------

final_df = (
    spark.read
    .format("delta")
    .load(delta_path)
)

print("Final Delta record count:", final_df.count())

final_df.orderBy("customer_id").show(truncate=False)


spark.stop()