from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, countDistinct, explode, col

spark = SparkSession.builder.appName("Steam Analysis Check").getOrCreate()

topsellers = spark.read.option("multiline", "true").json(
    "data_lake/raw/topsellers_*.json"
)

details = spark.read.option("multiline", "true").json(
    "data_lake/raw/appdetails_*.json"
)

df = topsellers.join(
    details,
    on=["app_id", "country_code"],
    how="inner"
)

df.printSchema()
#df.show(10, truncate=False)
df.show(10, truncate=True)