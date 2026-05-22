from pyspark.sql import SparkSession 
from pyspark.sql.functions import countDistinct, col, count, when
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

# Überprüfen, ob Tage felen
df_fullsteam.select(
    "t.scrape_date",
    "d.scrape_date"
).distinct().orderBy("t.scrape_date").show()

# Überprüfen, ob es fehlende Tage pro Land 
# -> 7. Mai nicht verwerten, besser löschen, war ohne overwrite
df_counts_per_day = df_fullsteam.groupBy("t.scrape_date") \
    .pivot("t.country_code") \
    .agg(count("*"))

df_counts_per_day.show()

print("Überprüfen, ob es pro APP-ID fehlende App-IDs gibt")
print()
df_app_id = df_fullsteam.groupBy("app_id") \
    .agg(
        countDistinct("name").alias("details_found")
    )

df_app_id.show(5)

print("Überprüfen, ob es fehlende App-IDs/Namen gibt")
total = df_fullsteam.count()
missing = df_fullsteam.filter(col("name").isNull()).count()
print()
print(f"Missing details: {missing}/{total}")
print()
# Dubletten Check
print()
print("Dubletten Check:")
print()
df_dubletten = df_fullsteam.groupBy("scrape_date", "country_code", "app_id") \
    .agg(
        count("app_id").alias("count_rows")
    ).filter(col("count_rows") > 1)

print("\n====================")
print("DUPLIKATE")
print("====================")

df_dubletten.orderBy("count_rows", ascending=False).show()
df_dubletten.count()

print("\n====================")
print("Fehlende Werte in wichtigsten Spalten")
print("====================")
important_cols = [
    "app_id",
    "name",
    "final_price_cents",
    "discount_percent"
]

df_fullsteam.select([
    count(
        when(col(c).isNull(), c)
    ).alias(c)
    for c in important_cols
]).show()