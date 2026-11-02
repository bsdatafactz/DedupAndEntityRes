import csv, os
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").appName("evaluate").getOrCreate()

silver = spark.read.parquet("data/silver")
gold = spark.read.parquet("data/gold")
gt = spark.read.option("header", True).csv("data/raw/ground_truth.csv")
full = silver.join(gt, "bronze_id")          # every silver row + its true_person_id
assert full.count() == silver.count(), "some silver rows have no ground truth"


def count_pairs(df, keys):
    """A group of n records holds n*(n-1)/2 pairs."""
    g = df.groupBy(*keys).count()
    total = g.agg(F.sum(F.col("count") * (F.col("count") - 1) / 2)).first()[0]
    return int(total or 0)


predicted_pairs = count_pairs(full, ["entity_id"])                   # pairs the pipeline linked
true_pairs = count_pairs(full, ["true_person_id"])                   # pairs that really match
tp = count_pairs(full, ["entity_id", "true_person_id"])              # linked AND really match
fp = predicted_pairs - tp                                            # linked but different people
fn = true_pairs - tp                                                 # same person but not linked

precision = tp / (tp + fp) if (tp + fp) else 0.0
recall = tp / (tp + fn) if (tp + fn) else 0.0
f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

n_records = full.count()
n_entities = full.select("entity_id").distinct().count()
n_people = full.select("true_person_id").distinct().count()
n_gold = gold.count()

print("=" * 60)
print(f"records:                     {n_records}")
print(f"entities found (pipeline):   {n_entities}")
print(f"real people (answer key):    {n_people}")
print(f"gold rows:                   {n_gold}")
print(f"TP={tp}  FP={fp}  FN={fn}")
print(f"precision={precision:.4f}  recall={recall:.4f}  f1={f1:.4f}")
print("=" * 60)

cols = ["entity_id", "true_person_id", "silver_id", "source_system", "first_name", "last_name",
        "dob", "ssn", "phone", "email", "street", "zip5", "match_rule"]

# ---------- wrong merges: one entity_id holding more than one real person ----------
wrong = (full.groupBy("entity_id").agg(F.countDistinct("true_person_id").alias("k"))
             .filter("k > 1").select("entity_id"))
print(f"\nWRONG MERGES: {wrong.count()} entities contain more than one real person")
full.join(wrong, "entity_id").select(cols).orderBy("entity_id", "true_person_id").show(20, truncate=False)

# ---------- missed matches: one real person spread over several entity_ids ----------
missed = (full.groupBy("true_person_id").agg(F.countDistinct("entity_id").alias("k"))
              .filter("k > 1").select("true_person_id"))
print(f"MISSED MATCHES: {missed.count()} real people are split across several entities")
full.join(missed, "true_person_id").select(cols).orderBy("true_person_id", "entity_id").show(30, truncate=False)

# ---------- which rules did the linking ----------
print("records linked by each rule:")
silver.groupBy("match_rule").count().orderBy(F.desc("count")).show()

# ---------- save the numbers for the Results slide ----------
os.makedirs("data/exports", exist_ok=True)
with open("data/exports/eval_metrics.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["metric", "value"])
    for k, v in [("records", n_records), ("entities_found", n_entities), ("real_people", n_people),
                 ("gold_rows", n_gold), ("true_positive_pairs", tp), ("false_positive_pairs", fp),
                 ("false_negative_pairs", fn), ("precision", round(precision, 4)),
                 ("recall", round(recall, 4)), ("f1", round(f1, 4))]:
        w.writerow([k, v])
print("saved data/exports/eval_metrics.csv")
spark.stop()
