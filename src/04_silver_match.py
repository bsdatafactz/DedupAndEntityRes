from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").appName("silver_match").getOrCreate()
s = spark.read.parquet("data/silver_clean")

# ---------- 1. blocking: candidate pairs share a dob; a < b avoids self-pairs and mirrors ----------
a, b = s.alias("a"), s.alias("b")
pairs = a.join(b, (F.col("a.dob") == F.col("b.dob")) & (F.col("a.silver_id") < F.col("b.silver_id")))

# ---------- 2. rules (NULL comparisons evaluate to NULL, so they never fire) ----------
r1 = F.col("a.ssn") == F.col("b.ssn")
r2 = (F.col("a.phone") == F.col("b.phone")) & (F.col("a.last_name") == F.col("b.last_name"))
r3 = F.col("a.email") == F.col("b.email")
r4 = ((F.col("a.last_name") == F.col("b.last_name"))
      & (F.levenshtein("a.first_name", "b.first_name") <= 2)
      & (F.col("a.zip5") == F.col("b.zip5")))

matches = (pairs
           .withColumn("rule", F.when(r1, "ssn_exact")
                                .when(r2, "phone_lastname")
                                .when(r3, "email_exact")
                                .when(r4, "name_fuzzy_zip"))
           .filter(F.col("rule").isNotNull())
           .select(F.col("a.silver_id").alias("id_a"), F.col("b.silver_id").alias("id_b"), "rule"))

pair_rows = matches.collect()
print("candidate pairs matched:", len(pair_rows))
matches.groupBy("rule").count().show()

# ---------- 3. cluster with union-find ----------
parent = {r["silver_id"]: r["silver_id"] for r in s.select("silver_id").collect()}

def find(x):
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x

def union(x, y):
    rx, ry = find(x), find(y)
    if rx != ry:
        parent[max(rx, ry)] = min(rx, ry)

rule_of = {}
for p in pair_rows:
    union(p["id_a"], p["id_b"])
    rule_of.setdefault(p["id_a"], p["rule"])
    rule_of.setdefault(p["id_b"], p["rule"])

# ---------- 4. entity_id per cluster ----------
roots = sorted({find(x) for x in parent})
entity_of_root = {r: f"E{i:05d}" for i, r in enumerate(roots, 1)}
mapping = [(sid, entity_of_root[find(sid)], rule_of.get(sid, "singleton")) for sid in parent]
map_df = spark.createDataFrame(mapping, ["silver_id", "entity_id", "match_rule"])

silver = s.join(map_df, "silver_id")
silver.write.mode("overwrite").parquet("data/silver")

# ---------- checks ----------
print("silver rows:", silver.count(), "| entities:", silver.select("entity_id").distinct().count())
print("cluster size distribution (records per entity):")
(silver.groupBy("entity_id").count().groupBy(F.col("count").alias("records_in_entity"))
       .count().orderBy("records_in_entity").show())
print("sample multi-record entities:")
big = silver.groupBy("entity_id").count().filter("count >= 3").limit(3).select("entity_id")
(silver.join(big, "entity_id")
       .select("entity_id", "silver_id", "source_system", "first_name", "last_name",
               "dob", "ssn", "phone", "email", "street", "zip5", "match_rule")
       .orderBy("entity_id", "silver_id").show(20, truncate=False))
spark.stop()