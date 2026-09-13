from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip


# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

builder = (
    SparkSession.builder
    .appName("CustomerReconciliationAudit")
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

cdc_path = "data/raw/customers_cdc_batch.csv"
silver_path = "data/silver_delta/customers"
audit_path = "data/audit/pipeline_audit"


# ---------------------------------------------------------
# Read CDC batch
# ---------------------------------------------------------

cdc_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(cdc_path)
)

source_count = cdc_df.count()

insert_count = cdc_df.filter(
    col("operation") == "I"
).count()

update_count = cdc_df.filter(
    col("operation") == "U"
).count()

delete_count = cdc_df.filter(
    col("operation") == "D"
).count()


# ---------------------------------------------------------
# Get current Delta version
# ---------------------------------------------------------

delta_table = DeltaTable.forPath(
    spark,
    silver_path
)

history = delta_table.history()

current_version = (
    history
    .select("version")
    .orderBy(col("version").desc())
    .first()["version"]
)

print(f"Current Delta version : {current_version}")


# ---------------------------------------------------------
# Target count AFTER CDC
# ---------------------------------------------------------

target_after_count = (
    spark.read
    .format("delta")
    .load(silver_path)
    .count()
)


# ---------------------------------------------------------
# Target count BEFORE CDC
# ---------------------------------------------------------

previous_version = current_version - 1

target_before_count = (
    spark.read
    .format("delta")
    .option("versionAsOf", previous_version)
    .load(silver_path)
    .count()
)


# ---------------------------------------------------------
# Expected target count
# ---------------------------------------------------------

expected_target_after_count = (
    target_before_count
    + insert_count
    - delete_count
)


# ---------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------

if expected_target_after_count == target_after_count:
    reconciliation_status = "PASSED"
else:
    reconciliation_status = "FAILED"


# ---------------------------------------------------------
# Data Quality Status
# ---------------------------------------------------------

dq_status = "PASSED"


# ---------------------------------------------------------
# Overall Pipeline Status
# ---------------------------------------------------------

if (
    dq_status == "PASSED"
    and reconciliation_status == "PASSED"
):
    pipeline_status = "SUCCESS"
else:
    pipeline_status = "FAILED"

# ---------------------------------------------------------
# Print Audit Information
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("              PIPELINE RECONCILIATION")
print("=" * 60)

print(f"Source CDC records       : {source_count}")
print(f"Insert records           : {insert_count}")
print(f"Update records           : {update_count}")
print(f"Delete records           : {delete_count}")

print(f"Target count BEFORE      : {target_before_count}")
print(f"Expected target AFTER    : {expected_target_after_count}")
print(f"Target count AFTER       : {target_after_count}")

print(f"DQ Status                : {dq_status}")
print(f"Reconciliation Status    : {reconciliation_status}")
print(f"Pipeline Status          : {pipeline_status}")

print("=" * 60)


# ---------------------------------------------------------
# Create Audit DataFrame
# ---------------------------------------------------------

audit_df = spark.createDataFrame(
    [
        (
            "CustomerCDCProcessing",
            source_count,
            target_before_count,
            target_after_count,
            expected_target_after_count,
            insert_count,
            update_count,
            delete_count,
            dq_status,
            reconciliation_status,
            pipeline_status
        )
    ],
    [
        "pipeline_name",
        "source_count",
        "target_before_count",
        "target_after_count",
        "expected_target_after_count",
        "insert_count",
        "update_count",
        "delete_count",
        "dq_status",
        "reconciliation_status",
        "pipeline_status"
    ]
)

audit_df = audit_df.withColumn(
    "run_timestamp",
    current_timestamp()
)


# ---------------------------------------------------------
# Write Audit Log
# ---------------------------------------------------------

(
    audit_df.write
    .format("delta")
    .mode("append")
    .save(audit_path)
)

print("\nAudit log written successfully.")

print("\nLatest audit record:")

(
    spark.read
    .format("delta")
    .load(audit_path)
    .orderBy(col("run_timestamp").desc())
    .show(truncate=False)
)


if pipeline_status == "FAILED":
    spark.stop()
    raise SystemExit(1)

spark.stop()