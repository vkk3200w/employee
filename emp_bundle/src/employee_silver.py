from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when
)

spark = SparkSession.builder.getOrCreate()

# ============================================================
# CONFIGURATION
# ============================================================

BRONZE_TABLE = "employee_attrition_bronze"
SILVER_TABLE = "employee_attrition_silver"
DQ_TABLE = "employee_attrition_data_quality"

# ============================================================
# READ BRONZE
# ============================================================

print("==========================================")
print("SILVER TRANSFORMATION STARTED")
print("==========================================")

df = spark.table(BRONZE_TABLE)

print(f"Bronze records: {df.count()}")

# ============================================================
# CLEANING & STANDARDIZATION
# ============================================================

silver_df = (
    df

    # --------------------------------------------------------
    # Trim string columns
    # --------------------------------------------------------

    .withColumn(
        "BusinessTravel",
        trim(col("BusinessTravel"))
    )

    .withColumn(
        "Department",
        trim(col("Department"))
    )

    .withColumn(
        "JobRole",
        trim(col("JobRole"))
    )

    .withColumn(
        "Gender",
        trim(col("Gender"))
    )

    # --------------------------------------------------------
    # Standardize categorical values
    # --------------------------------------------------------

    .withColumn(
        "Attrition",
        upper(trim(col("Attrition")))
    )

    .withColumn(
        "OverTime",
        upper(trim(col("OverTime")))
    )

    # --------------------------------------------------------
    # Create Age Group
    # --------------------------------------------------------

    .withColumn(
        "AgeGroup",
        when(col("Age") < 30, "Under 30")
        .when(col("Age") < 40, "30-39")
        .when(col("Age") < 50, "40-49")
        .otherwise("50+")
    )

    # --------------------------------------------------------
    # Create Income Band
    # --------------------------------------------------------

    .withColumn(
        "IncomeBand",
        when(col("MonthlyIncome") < 3000, "Low")
        .when(col("MonthlyIncome") < 7000, "Medium")
        .otherwise("High")
    )

    # --------------------------------------------------------
    # Overtime Flag
    # --------------------------------------------------------

    .withColumn(
        "OverTimeFlag",
        when(col("OverTime") == "YES", 1)
        .otherwise(0)
    )
)

# ============================================================
# REMOVE DUPLICATES
# ============================================================

before_dedup = silver_df.count()

silver_df = silver_df.dropDuplicates(
    ["EmployeeNumber"]
)

after_dedup = silver_df.count()

duplicates_removed = before_dedup - after_dedup

print(f"Records before deduplication: {before_dedup}")
print(f"Records after deduplication:  {after_dedup}")
print(f"Duplicates removed:          {duplicates_removed}")

# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("==========================================")
print("DATA QUALITY CHECKS")
print("==========================================")

# ------------------------------------------------------------
# Check 1: NULL Employee IDs
# ------------------------------------------------------------

null_employee_ids = silver_df.filter(
    col("EmployeeNumber").isNull()
).count()

# ------------------------------------------------------------
# Check 2: Duplicate Employee IDs
# ------------------------------------------------------------

duplicate_employee_ids = (
    silver_df
    .groupBy("EmployeeNumber")
    .count()
    .filter(col("count") > 1)
    .count()
)

# ------------------------------------------------------------
# Check 3: Invalid Attrition values
# ------------------------------------------------------------

invalid_attrition = silver_df.filter(
    ~col("Attrition").isin("YES", "NO")
).count()

# ------------------------------------------------------------
# Check 4: Invalid Overtime values
# ------------------------------------------------------------

invalid_overtime = silver_df.filter(
    ~col("OverTime").isin("YES", "NO")
).count()

# ------------------------------------------------------------
# Check 5: Invalid Age
# ------------------------------------------------------------

invalid_age = silver_df.filter(
    (col("Age") < 18) |
    (col("Age") > 100)
).count()

# ------------------------------------------------------------
# Check 6: Negative Income
# ------------------------------------------------------------

negative_income = silver_df.filter(
    col("MonthlyIncome") < 0
).count()

# ============================================================
# CREATE QUALITY REPORT
# ============================================================

checks = [
    (
        "NULL_EMPLOYEE_ID",
        0,
        null_employee_ids
    ),
    (
        "DUPLICATE_EMPLOYEE_ID",
        0,
        duplicate_employee_ids
    ),
    (
        "INVALID_ATTRITION",
        0,
        invalid_attrition
    ),
    (
        "INVALID_OVERTIME",
        0,
        invalid_overtime
    ),
    (
        "INVALID_AGE",
        0,
        invalid_age
    ),
    (
        "NEGATIVE_INCOME",
        0,
        negative_income
    )
]

results = []

for check_name, expected, actual in checks:

    status = (
        "PASS"
        if actual == expected
        else "FAIL"
    )

    results.append(
        (
            check_name,
            expected,
            actual,
            status
        )
    )

dq_df = spark.createDataFrame(
    results,
    [
        "check_name",
        "expected_value",
        "actual_value",
        "status"
    ]
)

# ============================================================
# SAVE DATA QUALITY REPORT
# ============================================================

(
    dq_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(DQ_TABLE)
)

print("==========================================")
print("DATA QUALITY REPORT")
print("==========================================")

dq_df.show(truncate=False)

# ============================================================
# QUALITY GATE
# ============================================================

failed_checks = dq_df.filter(
    col("status") == "FAIL"
).count()

if failed_checks > 0:

    print("==========================================")
    print("DATA QUALITY FAILED")
    print("==========================================")

    raise Exception(
        f"Data quality gate failed: "
        f"{failed_checks} check(s) failed."
    )

print("==========================================")
print("DATA QUALITY PASSED")
print("==========================================")

# ============================================================
# WRITE SILVER
# ============================================================

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_TABLE)
)

print(f"Silver table created: {SILVER_TABLE}")

print("==========================================")
print("SILVER TRANSFORMATION COMPLETED")
print("==========================================")