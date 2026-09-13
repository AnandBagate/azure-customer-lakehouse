from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("CustomerDeltaSilver")
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

input_path = "data/silver/customers"
delta_path = "data/silver_delta/customers"

print("Reading existing Silver data...")

df = spark.read.parquet(input_path)

print("Source Silver count:", df.count())

df.show(truncate=False)

# processed_at will be added later as an audit column
df = df.drop("processed_at")

print("Writing data as Delta...")

(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(delta_path)
)

print("Delta Silver table created successfully.")

delta_df = (
    spark.read
    .format("delta")
    .load(delta_path)
)

print("Delta Silver count:", delta_df.count())

delta_df.orderBy("customer_id").show(truncate=False)

spark.stop()