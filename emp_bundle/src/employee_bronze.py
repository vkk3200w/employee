from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ============================================================
# CONFIGURATION
# ============================================================

SOURCE_PATH = (
    "/Volumes/workspace/default/employee_csv"
)

BRONZE_TABLE = "employee_attrition_bronze"

# ============================================================
# BRONZE INGESTION
# ============================================================

print("==========================================")
print("BRONZE INGESTION STARTED")
print("==========================================")

print(f"Source: {SOURCE_PATH}")

raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(SOURCE_PATH)
)

print(f"Records read: {raw_df.count()}")

print("Source schema:")
raw_df.printSchema()

print("Sample data:")
raw_df.show(5, truncate=False)

# ============================================================
# WRITE BRONZE
# ============================================================

(
    raw_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)

print(f"Bronze table created: {BRONZE_TABLE}")

print("==========================================")
print("BRONZE INGESTION COMPLETED")
print("==========================================")