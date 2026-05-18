from pyspark.sql import SparkSession 
from pyspark.sql.functions import avg, countDistinct, col, explode, count
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
    .getOrCreate()

df_top = spark.read.parquet( "s3a://bde-steam-project-2026/processed/topsellers/" ) 
df_det = spark.read.parquet( "s3a://bde-steam-project-2026/processed/appdetails/" )

#df = df_top.join(df_det, ["app_id", "country_code"], "left")
df_fullsteam = df_top.alias("t").join(
    df_det.alias("d"),
    on=["app_id", "country_code", "scrape_date"],
    how="left"
)

#df.describe().show()
#df.printSchema()

df_fullsteam.select(
    "t.*",
    "d.name",
    "d.final_price_cents",
    "d.scrape_timestamp",
    "d.genres"
).show()

#df_top.select("scrape_date").distinct().orderBy("scrape_date").show()

df_fullsteam.select(
    "t.scrape_date",
    "d.scrape_date"
).distinct().orderBy("t.scrape_date").show()

df_fullsteam.select(
    "t.scrape_date",
    "t.country_code",
    "d.scrape_date",
    "d.country_code"
).distinct().orderBy("t.country_code", "t.scrape_date").show()

# Überprüfen, ob es fehlende Werte in den Join-Spalten gibt -> 7. Mai nicht verwerten, besser löschen
df_counts_per_day = df_fullsteam.groupBy("t.scrape_date") \
    .pivot("t.country_code") \
    .agg(count("*"))

df_counts_per_day.show()