from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

RAW_PATH_META = f"file://{BASE_DIR}/data_lake/raw/appdetails_*.json"
RAW_PATH_TOPSELLERS = f"file://{BASE_DIR}/data_lake/raw/topsellers_*.json"

print("RAW_PATH_META:", RAW_PATH_META)
print("RAW_PATH_TOPSELLERS:", RAW_PATH_TOPSELLERS)


spark = SparkSession.builder \
    .appName("ETL") \
    .getOrCreate()


# JSON laden
df_meta = spark.read \
    .option("multiline", "true") \
    .json(RAW_PATH_META)

df_topsellers = spark.read \
    .option("multiline", "true") \
    .json(RAW_PATH_TOPSELLERS)


# scrape_date aus Timestamp erzeugen
df_meta = df_meta.withColumn(
    "scrape_date",
    to_date(col("scrape_timestamp"))
)


# Parquet schreiben
df_meta.write \
    .mode("overwrite") \
    .partitionBy("scrape_date") \
    .parquet("data_lake/processed/appdetails")


df_topsellers.write \
    .mode("overwrite") \
    .partitionBy("scrape_date") \
    .parquet("data_lake/processed/topsellers")


# Kontrolle
df_meta.show(5)
df_topsellers.show(5)