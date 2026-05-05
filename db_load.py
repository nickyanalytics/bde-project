from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DB Load") \
    .getOrCreate()

df = spark.read.parquet("data_lake/processed/books")

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "books") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()