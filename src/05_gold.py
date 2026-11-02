from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("gold").getOrCreate()
s = spark.read.parquet("data/silver")

# ---------- helper: most frequent non-null value per entity ----------
def mode_of(col):
    counts = (s.filter(F.col(col).isNotNull())
               .groupBy("entity_id", col)
               .agg(F.count("*").alias("n"), F.max("ingested_at").alias("last_seen")))
    w = Window.partitionBy("entity_id").orderBy(F.col("n").desc(), F.col("last_seen").desc(), F.col(col))
    return (counts.withColumn("rn", F.row_number().over(w))
                  .filter("rn = 1")
                  .select("entity_id", col))

# ---------- latest address per entity ----------
w_latest = Window.partitionBy("entity_id").orderBy(F.col("ingested_at").desc(), F.col("silver_id").desc())
address = (s.withColumn("rn", F.row_number().over(w_latest))
            .filter("rn = 1")
            .select("entity_id", "street", "city", "state", "zip5"))

# ---------- counts and trace columns ----------
base = (s.groupBy("entity_id")
         .agg(F.count("*").alias("record_count"),
              F.countDistinct("source_system").alias("source_count"),
              F.concat_ws(",", F.sort_array(F.collect_list("silver_id"))).alias("silver_ids"),
              F.concat_ws(",", F.sort_array(F.collect_list("bronze_id"))).alias("bronze_ids")))

# ---------- assemble ----------
gold = base.join(address, "entity_id", "left")
for c in ["first_name", "last_name", "dob", "ssn", "phone", "email"]:
    gold = gold.join(mode_of(c), "entity_id", "left")

w_id = Window.orderBy("entity_id")
gold = gold.select(
    F.concat(F.lit("G"), F.lpad(F.row_number().over(w_id).cast("string"), 5, "0")).alias("gold_id"),
    "entity_id",
    "first_name", "last_name", "dob", "ssn", "phone", "email",
    "street", "city", "state", "zip5",
    "record_count", "source_count", "silver_ids", "bronze_ids")

gold.write.mode("overwrite").parquet("data/gold")

# ---------- checks ----------
print("gold rows (unique people):", gold.count(), "| silver rows:", s.count())
print("distinct gold_id:", gold.select("gold_id").distinct().count())
print("records merged per person:")
gold.groupBy("record_count").count().orderBy("record_count").show()
print("NULL count per column:")
gold.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in gold.columns]).show(truncate=False)
print("sample golden records with trace back:")
gold.filter("record_count >= 3").select(
    "gold_id", "first_name", "last_name", "dob", "phone", "email", "street",
    "record_count", "source_count", "silver_ids", "bronze_ids").show(5, truncate=False)
spark.stop()