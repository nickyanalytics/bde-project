from pyspark.sql import SparkSession 
from pyspark.sql.functions import avg, countDistinct, col, explode 
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

df_top = spark.read.option("multiline", "true").json( "s3a://bde-steam-project-2026/raw/topsellers_*.json" ) 
df_det = spark.read.option("multiline", "true").json( "s3a://bde-steam-project-2026/raw/appdetails_*.json" )

#df = df_top.join(df_det, ["app_id", "country_code"], "left")
df = df_top.alias("t").join(
    df_det.alias("d"),
    on=["app_id", "country_code"],
    how="left"
)

#df.describe().show()
df.printSchema()

df.select(
    "t.*",
    "d.name",
    "d.final_price_cents",
    "d.scrape_timestamp",
    "d.genres"
).show()

df_top.select("scrape_date").distinct().orderBy("scrape_date").show()
