from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Steam Quality Check") \
    .getOrCreate()

df_top = spark.read.option("multiline", "true").json(
    "data_lake/raw/topsellers_*.json"
)

df_det = spark.read.option("multiline", "true").json(
    "data_lake/raw/appdetails_*.json"
)

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
