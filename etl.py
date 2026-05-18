from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pathlib import Path

from dotenv import load_dotenv 
import os 

load_dotenv()

spark = SparkSession.builder \
    .appName("Steam Analysis Quality") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ) \
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.getenv("AWS_ACCESS_KEY_ID")
    ) \
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.getenv("AWS_SECRET_ACCESS_KEY")
    ) \
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "s3.eu-central-1.amazonaws.com"
    ) \
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ) \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ) \
    .config(
        "spark.hadoop.fs.s3a.connection.timeout",
        "200000"
    ) \
    .config(
        "spark.hadoop.fs.s3a.threads.keepalivetime",
        "60000"
    ) \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()


# JSON laden
df_meta = spark.read \
    .option("multiline", "true") \
    .json("s3a://bde-steam-project-2026/raw/appdetails_*.json")

df_topsellers = spark.read \
    .option("multiline", "true") \
    .json("s3a://bde-steam-project-2026/raw/topsellers_*.json")


# scrape_date aus Timestamp erzeugen
df_meta = df_meta.withColumn(
    "scrape_date",
    to_date(col("scrape_timestamp"))
)


# Parquet schreiben
df_meta.write \
    .mode("overwrite") \
    .partitionBy("scrape_date") \
    .parquet("s3a://bde-steam-project-2026/processed/appdetails")


df_topsellers.write \
    .mode("overwrite") \
    .partitionBy("scrape_date") \
    .parquet("s3a://bde-steam-project-2026/processed/topsellers")


# Kontrolle
df_meta.show(5)
df_topsellers.show(5)