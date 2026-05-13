from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import to_date, col
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RAW_PATH_meta = f"file://{BASE_DIR}/data_lake/raw/appdetails_*.json"
RAW_PATH_topsellers = f"file://{BASE_DIR}/data_lake/raw/topsellers_*.json"

print("RAW_PATH_meta:", RAW_PATH_meta)
print("RAW_PATH_topsellers:", RAW_PATH_topsellers)

spark = SparkSession.builder \
    .appName("ETL") \
    .getOrCreate()

# JSON laden
#json listen laden (Komma getrennt)
#df = spark.read.json("data_lake/raw/*.json")
#json lines laden, liest immer alle jsons ein (wichtig für die spätere Automatisierung, da wir ja jeden Tag neue Daten haben)
#df_meta = spark.read.option("multiline", "true").json("data_lake/raw/appdetails*.json")
df_meta = spark.read.option("multiline", "true").json(RAW_PATH_meta)
df_topsellers = spark.read.option("multiline", "true").json(RAW_PATH_topsellers)

#df_meta.printSchema()
#df_meta.show(5, truncate=False)

# speichern als Parquet (overwrtite erlaubt, damit wir bei jedem Lauf die alten Daten überschreiben)
df_meta.write.mode("overwrite").parquet("data_lake/processed/appdetails")
df_topsellers.write.mode("overwrite").parquet("data_lake/processed/topsellers")

df_metadaten = df_meta.withColumn("scrape_date", to_date(col("scrape_timestamp")))

df_metadaten.write \
  .mode("overwrite") \
  .partitionBy("scrape_date") \
  .parquet("data_lake/processed/appdetails")

df_topsellers.write \
  .mode("overwrite") \
  .partitionBy("scrape_date") \
  .parquet("data_lake/processed/topsellers")

df_metadaten.show(5)
df_topsellers.show(5)