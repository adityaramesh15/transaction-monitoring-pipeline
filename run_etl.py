from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, TimestampType

spark = (
    SparkSession.builder
    .appName("AML ETL Job")
    .master("local[4]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

df = spark.read.csv('raw_data/transactions.csv',
                    header = True,
                    inferSchema = True)

df_cleaned = (df.withColumn("amount", df["amount"].cast(DoubleType()))
                .withColumn("timestamp", df["timestamp"].cast(TimestampType()))
              )

df_cleaned.write.mode("overwrite").parquet("transactions_data.parquet")
spark.stop()