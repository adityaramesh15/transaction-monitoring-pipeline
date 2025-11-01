from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

print("Starting AML Job")

spark = (
    SparkSession.builder
    .appName("AML ETL Job")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

df = spark.read.parquet("transactions_data.parquet")

print("Creating simulated 'Hive' table view: 'transactions'")
df.createOrReplaceTempView("transactions")

print("Running AML Rule 1: High-Value")
rule1_flags = spark.sql("""
    SELECT transaction_id, 
            'High Value' as rule_name, 
            'Amount' as rule_desc
    FROM transactions
    WHERE amount > 10000
""")

print("Running AML Rule 2: High-Risk Jurisdiction")
high_risk_countries = "('Country A', 'Country B', 'Country C')"
rule2_flags = spark.sql(f"""
    SELECT transaction_id, 
            'High Risk Jurisdiction' as rule_name, 
            'Sent to High-Risk Country' as rule_desc
    FROM transactions
    WHERE to_jurisdiction IN {high_risk_countries}
""")

print("Running AML Rule 3: Rapid Movement")
rapid_movement_flags = spark.sql("""
    WITH account_activity AS (
        SELECT transaction_id, from_account,
                timestamp, amount,
            LAG(timestamp, 1) OVER (
                PARTITION BY from_account 
                ORDER BY timestamp
            ) as prev_transaction_time
        FROM transactions
    )
    SELECT
        transaction_id,
        'Rapid Movement' as rule_name,
        'Sent funds < 6hr after previous tx' as rule_desc
    FROM account_activity
    WHERE
        prev_transaction_time IS NOT NULL 
        AND
        (unix_timestamp(timestamp) - unix_timestamp(prev_transaction_time)) < 21600
""")

suspicious_activity_report = rule1_flags.union(rule2_flags).union(rapid_movement_flags)
suspicious_activity_report.show(20, truncate=False)

suspicious_activity_report.write \
    .mode("overwrite") \
    .csv("suspicious_activity_report.csv", header=True)

print("Analysis complete. Report saved.")
spark.stop