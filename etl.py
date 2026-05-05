from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import to_date, col

spark = SparkSession.builder \
    .appName("ETL") \
    .getOrCreate()

# JSON laden
#json listen laden (Komma getrennt)
#df = spark.read.json("data_lake/raw/*.json")
#json lines laden, liest immer alle jsons ein (wichtig für die spätere Automatisierung, da wir ja jeden Tag neue Daten haben)
df = spark.read.option("multiline", "true").json("data_lake/raw/*.json")

df.printSchema()
df.show(5, truncate=False)

# Preis aus Preis mit Währung extrahieren (z.B. £51.77 → 51.77)
df: DataFrame = df.withColumn(
    "price_numeric",
    regexp_extract("price", r"(\d+\.\d+)", 1).cast("float")
) 

# speichern als Parquet (overwrtite erlaubt, damit wir bei jedem Lauf die alten Daten überschreiben)
#df.write.mode("overwrite").parquet("data_lake/processed/books")

df = df.withColumn("scrape_date", to_date(col("scrape_time")))

df.write \
  .mode("overwrite") \
  .partitionBy("scrape_date") \
  .parquet("data_lake/processed/books")

df.show()