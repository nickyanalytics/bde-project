from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DB Load") \
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    ) \
    .getOrCreate()

df = spark.read.parquet("data_lake/processed/appdetails/")

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/steam_db") \
    .option("dbtable", "appdetails") \
    .option("user", "bde") \
    .option("password", "bde2026") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()