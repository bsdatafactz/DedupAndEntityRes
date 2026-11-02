from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.master("local[*]").appName("silver_standardize").getOrCreate()
b = spark.read.parquet("data/bronze")

# ---------- lookup tables ----------
NICK_TO_FORMAL = {
    "BILL": "WILLIAM", "WILL": "WILLIAM", "BOB": "ROBERT", "ROB": "ROBERT",
    "JIM": "JAMES", "JIMMY": "JAMES", "LIZ": "ELIZABETH", "BETH": "ELIZABETH",
    "MIKE": "MICHAEL", "JEN": "JENNIFER", "JENNY": "JENNIFER", "RICK": "RICHARD",
    "KATE": "KATHERINE", "KATIE": "KATHERINE", "TOM": "THOMAS",
    "MAGGIE": "MARGARET", "MEG": "MARGARET",
}
STATES = {"CALIFORNIA": "CA", "TEXAS": "TX", "NEW YORK": "NY", "FLORIDA": "FL", "ILLINOIS": "IL"}
STREET_ABBR = {"STREET": "ST", "AVENUE": "AVE", "ROAD": "RD", "DRIVE": "DR", "LANE": "LN",
               "BOULEVARD": "BLVD", "COURT": "CT", "APARTMENT": "APT", "SUITE": "STE"}

# ---------- 1. NAME: "Smith, Bill" / "BILL SMITH" / "  Bill   Smith " -> FIRST, LAST ----------
b = b.withColumn("_name", F.regexp_replace(F.trim("full_name"), r"\s+", " "))
has_comma = F.col("_name").contains(",")
comma_parts = F.split("_name", ",")
space_parts = F.split("_name", " ")

first_raw = F.when(has_comma, F.trim(comma_parts.getItem(1))).otherwise(space_parts.getItem(0))
last_raw = F.when(has_comma, F.trim(comma_parts.getItem(0))).otherwise(F.element_at(space_parts, -1))

def clean_letters(c):
    return F.regexp_replace(F.upper(c), r"[^A-Z' -]", "")

b = (b.withColumn("_first", clean_letters(first_raw))
      .withColumn("last_name", clean_letters(last_raw)))

# nickname -> formal name (join against a small lookup table)
nick_df = spark.createDataFrame(list(NICK_TO_FORMAL.items()), ["nick", "formal"])
b = (b.join(F.broadcast(nick_df), b["_first"] == nick_df["nick"], "left")
      .withColumn("first_name", F.coalesce("formal", "_first"))
      .drop("nick", "formal", "_first", "_name"))

# ---------- 2. DOB: 3 formats -> one DATE ----------
d = F.trim("dob")
dob = (F.when(d.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(d, "yyyy-MM-dd"))
        .when(d.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(d, "MM/dd/yyyy"))
        .when(d.rlike(r"^\d{2}-[A-Za-z]{3}-\d{4}$"), F.to_date(d, "dd-MMM-yyyy")))

# ---------- 3. SSN: digits only, must be 9 ----------
ssn_digits = F.regexp_replace("ssn", r"[^0-9]", "")
ssn = F.when(F.length(ssn_digits) == 9, ssn_digits)

# ---------- 4. PHONE: digits only, drop leading country code 1, must be 10 ----------
ph = F.regexp_replace("phone", r"[^0-9]", "")
phone = (F.when(F.length(ph) == 10, ph)
          .when((F.length(ph) == 11) & ph.startswith("1"), ph.substr(2, 10)))

# ---------- 5. EMAIL: lowercase, trimmed, must contain @ ----------
em = F.lower(F.trim("email"))
email = F.when(em.contains("@"), em)

# ---------- 6. STREET: uppercase, no punctuation, abbreviations ----------
street = F.regexp_replace(F.upper(F.trim("address")), r"[.,]", "")
for long_form, short_form in STREET_ABBR.items():
    street = F.regexp_replace(street, rf"\b{long_form}\b", short_form)
street = F.trim(F.regexp_replace(street, r"\s+", " "))

# ---------- 7. CITY ----------
city = F.trim(F.regexp_replace(F.upper(F.trim("city")), r"\s+", " "))

# ---------- 8. STATE: "Texas" / "tx" -> TX ----------
st = F.upper(F.trim("state"))
state = F.when(F.length(st) == 2, st)
for full_name, abbr in STATES.items():
    state = state.when(st == full_name, abbr)

# ---------- 9. ZIP: first 5 digits ----------
zp = F.regexp_replace("zip", r"[^0-9]", "")
zip5 = F.when(F.length(zp) >= 5, F.substring(zp, 1, 5))

# ---------- assemble silver ----------
w = Window.orderBy("bronze_id")
silver = (b.select(
            F.concat(F.lit("S"), F.lpad(F.row_number().over(w).cast("string"), 5, "0")).alias("silver_id"),
            "bronze_id", "source_system", "source_customer_id",
            "first_name", "last_name",
            dob.alias("dob"), ssn.alias("ssn"), phone.alias("phone"), email.alias("email"),
            street.alias("street"), city.alias("city"), state.alias("state"), zip5.alias("zip5"),
            F.to_date("ingested_at").alias("ingested_at")))

silver.write.mode("overwrite").parquet("data/silver_clean")

# ---------- quick checks ----------
print("silver rows:", silver.count(), "| bronze rows:", b.count())
print("distinct silver_id:", silver.select("silver_id").distinct().count())

print("NULL count per column (compare with bronze NULLs):")
silver.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in silver.columns]).show(truncate=False)

print("bad formats (all should be 0):")
silver.select(
    F.sum((F.length("phone") != 10).cast("int")).alias("bad_phone"),
    F.sum((F.length("ssn") != 9).cast("int")).alias("bad_ssn"),
    F.sum((F.length("zip5") != 5).cast("int")).alias("bad_zip"),
    F.sum((F.length("state") != 2).cast("int")).alias("bad_state"),
).show()

print("before vs after:")
raw = (spark.read.parquet("data/bronze")
       .select("bronze_id",
               F.col("full_name").alias("raw_name"),
               F.col("dob").alias("raw_dob"),
               F.col("phone").alias("raw_phone"),
               F.col("address").alias("raw_address"),
               F.col("state").alias("raw_state")))

(silver.join(raw, "bronze_id")
       .select("bronze_id", "raw_name", "first_name", "last_name",
               "raw_dob", "dob", "raw_phone", "phone",
               "raw_address", "street", "raw_state", "state")
       .show(15, truncate=False))