import os, shutil
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[*]").appName("publish")
         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
         .getOrCreate())

URL = "jdbc:postgresql://localhost:5432/meridian"
PROPS = {"user": os.environ["USER"], "driver": "org.postgresql.Driver"}
os.makedirs("data/exports", exist_ok=True)

LAYERS = {"bronze": "bronze.customers", "silver": "silver.customers", "gold": "gold.customers"}

for layer, table in LAYERS.items():
    path = f"data/{layer}"
    if not os.path.exists(path):
        print(f"SKIP {layer}: {path} not found")
        continue
    df = spark.read.parquet(path)
    df.write.jdbc(URL, table, mode="overwrite", properties=PROPS)      # SQL table
    df.toPandas().to_csv(f"data/exports/{layer}.csv", index=False)     # CSV file
    print(f"{layer}: {df.count()} rows -> table {table} + data/exports/{layer}.csv")

gt = spark.read.option("header", True).csv("data/raw/ground_truth.csv")
gt.write.jdbc(URL, "eval.ground_truth", mode="overwrite", properties=PROPS)
shutil.copy("data/raw/ground_truth.csv", "data/exports/ground_truth.csv")
print(f"ground_truth: {gt.count()} rows -> table eval.ground_truth + data/exports/ground_truth.csv")

spark.stop()
