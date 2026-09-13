from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CustomerBronzeIngestion")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/raw/customers.csv"
output_path = "data/bronze/customers"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

print("Source data:")
df.show()

print("Writing data to Bronze layer...")

df.write \
    .mode("overwrite") \
    .parquet(output_path)

print("Bronze ingestion completed successfully.")

spark.stop()