from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("bronze").getOrCreate()

bronze = (spark.read
          .option("header", True)
          .option("inferSchema", False)      # keep everything as strings, no auto-fixing
          .csv("data/raw/bronze_raw.csv"))

bronze.printSchema()
print("rows:", bronze.count())
print("distinct bronze_id:", bronze.select("bronze_id").distinct().count())  # must equal rows

bronze.write.mode("overwrite").parquet("data/bronze")
spark.stop()