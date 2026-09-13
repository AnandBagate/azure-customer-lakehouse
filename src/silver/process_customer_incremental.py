from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    current_timestamp,
    row_number
)
from pyspark.sql.window import Window


# --------------------------------------------------
# 1. Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("CustomerIncrementalProcessing")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# 2. Paths
# --------------------------------------------------

existing_path = "data/silver/customers"
batch_path = "data/raw/customers_batch2.csv"
output_path = "data/silver/customers"


# --------------------------------------------------
# 3. Read existing Silver data
# --------------------------------------------------

existing_df = spark.read.parquet(existing_path)

print("Existing Silver count:", existing_df.count())


# --------------------------------------------------
# 4. Read new incoming batch
# --------------------------------------------------

batch_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(batch_path)
)

print("Incoming batch count:", batch_df.count())

print("Incoming batch:")
batch_df.show(truncate=False)


# --------------------------------------------------
# 5. Data quality filtering
#    customer_id and email are mandatory
# --------------------------------------------------

batch_clean = batch_df.filter(
    col("customer_id").isNotNull()
    & col("email").isNotNull()
)


# --------------------------------------------------
# 6. Standardize columns
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
# 7. Deduplicate incoming batch
#    Keep latest record for each customer_id
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


# --------------------------------------------------
# 8. Remove audit column from old Silver
#    before combining datasets
# --------------------------------------------------

existing_df = existing_df.drop("processed_at")


# --------------------------------------------------
# 9. Combine existing + incoming records
# --------------------------------------------------

combined_df = existing_df.unionByName(batch_latest)


# --------------------------------------------------
# 10. Keep latest version of every customer
# --------------------------------------------------

final_window = (
    Window
    .partitionBy("customer_id")
    .orderBy(col("updated_at").desc())
)

final_df = (
    combined_df
    .withColumn("row_number", row_number().over(final_window))
    .filter(col("row_number") == 1)
    .drop("row_number")
)


# --------------------------------------------------
# 11. Add processing timestamp
# --------------------------------------------------

final_df = final_df.withColumn(
    "processed_at",
    current_timestamp()
)


# --------------------------------------------------
# 12. Display result
# --------------------------------------------------

print("Final Silver count:", final_df.count())

print("Final Silver data:")

final_df.orderBy("customer_id").show(truncate=False)


# --------------------------------------------------
# 13. Write updated Silver
# --------------------------------------------------

print("Writing updated Silver layer...")

final_df.write \
    .mode("overwrite") \
    .parquet(output_path)

print("Incremental processing completed successfully.")


# --------------------------------------------------
# 14. Stop Spark
# --------------------------------------------------

spark.stop()