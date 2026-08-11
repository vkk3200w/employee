from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    count,
    avg,
    round as spark_round,
    sum as spark_sum
)

spark = SparkSession.builder.getOrCreate()

SILVER_TABLE = "employee_attrition_silver"

GOLD_EMPLOYEE_TABLE = "employee_attrition_gold"

GOLD_SUMMARY_TABLE = "employee_attrition_summary"

print("==========================================")
print("GOLD ANALYTICS STARTED")
print("==========================================")

df = spark.table(SILVER_TABLE)

# ============================================================
# ATTRITION RISK SCORE
# ============================================================

risk_df = (
    df

    # Overtime = YES → +2
    .withColumn(
        "overtime_points",
        when(
            col("OverTime") == "YES",
            2
        ).otherwise(0)
    )

    # Job Satisfaction <= 2 → +2
    .withColumn(
        "satisfaction_points",
        when(
            col("JobSatisfaction") <= 2,
            2
        ).otherwise(0)
    )

    # Monthly Income < 3000 → +2
    .withColumn(
        "income_points",
        when(
            col("MonthlyIncome") < 3000,
            2
        ).otherwise(0)
    )

    # Work Life Balance <= 2 → +1
    .withColumn(
        "worklife_points",
        when(
            col("WorkLifeBalance") <= 2,
            1
        ).otherwise(0)
    )

    # Distance from home > 15 → +1
    .withColumn(
        "distance_points",
        when(
            col("DistanceFromHome") > 15,
            1
        ).otherwise(0)
    )

    # Years at company < 3 → +2
    .withColumn(
        "tenure_points",
        when(
            col("YearsAtCompany") < 3,
            2
        ).otherwise(0)
    )
)

# ============================================================
# TOTAL RISK SCORE
# ============================================================

risk_df = risk_df.withColumn(
    "risk_score",
    col("overtime_points")
    + col("satisfaction_points")
    + col("income_points")
    + col("worklife_points")
    + col("distance_points")
    + col("tenure_points")
)

# ============================================================
# RISK CATEGORY
# ============================================================

risk_df = risk_df.withColumn(
    "risk_category",
    when(
        col("risk_score") >= 6,
        "HIGH"
    )
    .when(
        col("risk_score") >= 3,
        "MEDIUM"
    )
    .otherwise("LOW")
)

# ============================================================
# GOLD EMPLOYEE TABLE
# ============================================================

gold_df = risk_df.select(
    "EmployeeNumber",
    "Age",
    "AgeGroup",
    "Gender",
    "Department",
    "JobRole",
    "MonthlyIncome",
    "IncomeBand",
    "OverTime",
    "JobSatisfaction",
    "WorkLifeBalance",
    "DistanceFromHome",
    "YearsAtCompany",
    "Attrition",
    "risk_score",
    "risk_category"
)

(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_EMPLOYEE_TABLE)
)

# ============================================================
# GOLD SUMMARY
# ============================================================

summary_df = (
    gold_df
    .groupBy("risk_category")
    .agg(
        count("*").alias("employee_count"),

        spark_round(
            avg("MonthlyIncome"),
            2
        ).alias("avg_monthly_income"),

        spark_round(
            avg("YearsAtCompany"),
            2
        ).alias("avg_years_at_company"),

        spark_sum(
            when(
                col("Attrition") == "YES",
                1
            ).otherwise(0)
        ).alias("attrition_count")
    )
)

(
    summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_SUMMARY_TABLE)
)

print("==========================================")
print("ATTRITION RISK SUMMARY")
print("==========================================")

summary_df.show()

print("==========================================")
print("GOLD ANALYTICS COMPLETED")
print("==========================================")