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
# 1. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("CustomerSilverTransformation")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# 2. Paths
# --------------------------------------------------

input_path = "data/bronze/customers"
output_path = "data/silver/customers"


# --------------------------------------------------
# 3. Read Bronze
# --------------------------------------------------

df = spark.read.parquet(input_path)

print("Bronze record count:", df.count())

print("Bronze data:")
df.show()


# --------------------------------------------------
# 4. Data quality - remove records with NULL
#    customer_id or email
# --------------------------------------------------

df_clean = df.filter(
    col("customer_id").isNotNull()
    & col("email").isNotNull()
)


# --------------------------------------------------
# 5. Standardize string columns
# --------------------------------------------------

df_clean = (
    df_clean
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", trim(col("email")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("country", trim(col("country")))
    .withColumn("status", upper(trim(col("status"))))
)


# --------------------------------------------------
# 6. Deduplicate customers
#    Keep the latest record based on updated_at
# --------------------------------------------------

window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(col("updated_at").desc())
)

df_clean = (
    df_clean
    .withColumn("row_number", row_number().over(window_spec))
    .filter(col("row_number") == 1)
    .drop("row_number")
)


# --------------------------------------------------
# 7. Add audit timestamp
# --------------------------------------------------

df_clean = df_clean.withColumn(
    "processed_at",
    current_timestamp()
)


# --------------------------------------------------
# 8. Display Silver data
# --------------------------------------------------

print("Silver record count:", df_clean.count())

print("Silver data:")
df_clean.show(truncate=False)


# --------------------------------------------------
# 9. Write Silver
# --------------------------------------------------

print("Writing data to Silver layer...")

df_clean.write \
    .mode("overwrite") \
    .parquet(output_path)

print("Silver transformation completed successfully.")


# --------------------------------------------------
# 10. Stop Spark
# --------------------------------------------------

spark.stop()