from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CustomerLakehouse") \
    .master("local[*]") \
    .getOrCreate()

input_path = "data/raw/customers.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

df.printSchema()

df.show()

spark.stop()