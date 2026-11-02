# Deduplication and Entity Resolution with Medallion Architecture

Project notes for a financial services use case, built with Python and PySpark running locally.

---

## 1. Task Overview (Business Use Case)

**Company (fictional):** Meridian Trust Bank, a regional bank.

**Situation:** Customer data lives in three separate systems:

| Source system | What it holds |
|---|---|
| core_banking | Checking and savings customers |
| credit_card | Card customers |
| loan | Loan and mortgage customers |

Each system has its own customer ID and its own data entry habits. Nothing links them. The same real person can appear:
- in several systems (for example, once in core banking and once in loans), and
- more than once inside the same system (a duplicate load).

**Business question:** How many real customers do we have, and what is the best record for each one?

**Why it matters for a bank:**
- **Sanctions and AML screening** needs one identity per person. If a person is split across records, a risky match can be missed.
- **Customer counts and regulatory reports** are overstated when duplicates exist.
- **Customer 360 and credit exposure** are wrong when one person's products are spread across several records.
- **Duplicate onboarding** repeats KYC work for people who are already customers.

### Two related tasks

| Task | Meaning | Example |
|---|---|---|
| **Deduplication** | Remove copies of the same record inside one source | Core banking loaded the same customer twice |
| **Entity resolution** | Link records across sources that describe the same real person, even with no shared ID | "Bill Smith" in loans and "WILLIAM SMITH" in credit cards |

Both use the same pipeline. Deduplication is the easier case, and entity resolution is the harder one.

---

## 2. Requirements

**Business requirements**
- Produce one trusted record per real person.
- Be able to explain, for any person, which source records were combined and why.
- Reduce duplicates in customer data to near zero.

**Data requirements**
- Keep the original raw data unchanged.
- Standardize formats (phone, date, SSN, address, name, state, ZIP) into one form each.
- Treat missing values (NULL) as "no evidence", never as a mismatch.

**Quality requirements**
- Measure accuracy against a known answer set, using precision, recall, and F1.
- Avoid wrongly merging two different people (false positive).
- Avoid missing true matches (false negative).

**Traceability requirement**
- Every layer has its own ID, and each layer points back to the one below it.

---

## 3. Approach

### 3.1 Why medallion architecture (bronze, silver, gold)

Deduplication and entity resolution are not one step. They are a chain of transformations, and each step needs a different guarantee about the data. The layers match that chain.

| Layer | Purpose | Guarantee |
|---|---|---|
| **Bronze** | Raw data exactly as received | Never modified |
| **Silver** | Cleaned, standardized, and linked by entity | Nothing deleted, every decision recorded |
| **Gold** | One best record per real person | Business-ready, trusted |

Reasons this fits the problem:
- **Reprocessability:** matching can be wrong. Because bronze is untouched, we can change the rules and rebuild silver and gold.
- **Auditability:** we can answer "why were these two records merged?" because the match rule is stored in silver.
- **Separation of concerns:** cleaning, matching, and choosing the best value are separate steps that can be tested on their own.
- **Cleaning before matching:** "St." versus "Street" or different phone formats would prevent matches, so standardization has to come first.

### 3.2 Three tables, three IDs

The design is deliberately simple: one table per layer, and one primary key per layer.

| Layer | Primary key | Links to |
|---|---|---|
| Bronze | bronze_id | none (it is the starting point) |
| Silver | silver_id | bronze_id points back to bronze; entity_id says which real person the row belongs to |
| Gold | gold_id | entity_id matches silver; lists of silver_ids and bronze_ids give the full trace |

**Key design decisions:**
- **Silver has the same number of rows as bronze.** Silver never deletes. Duplicates and cross-system matches simply receive the same entity_id.
- **Gold has one row per real person.** This is where the duplicates collapse.
- **Silver holds the match decision.** It stores entity_id and the rule that linked the record (match_rule).
- **Gold stores the trace.** From a gold row you can reach its silver rows, and from there the raw bronze rows.

### 3.3 What each layer does

**Bronze: raw**
- Load the source data with all columns kept as text, so nothing is auto-corrected.
- Add nothing except the ID and ingestion date that came with the data.
- The bronze values must stay identical to the source values.

**Silver: standardize, then match**

Step A, standardize each column to a single form:

| Field | Problem in raw data | Standard form |
|---|---|---|
| Name | "Smith, Bill", "BILL SMITH", extra spaces | Uppercase first name and last name, nickname mapped to the formal name (BILL becomes WILLIAM) |
| Date of birth | Three different date formats | One real date type |
| SSN | With dashes or without | Nine digits only, otherwise NULL |
| Phone | Four formats, sometimes with a country code | Ten digits only, otherwise NULL |
| Email | Mixed case | Lowercase, must contain an @ |
| Street | Mixed case, abbreviations, punctuation | Uppercase, no punctuation, STREET becomes ST and similar |
| State | "Texas" or "TX" | Two-letter code |
| ZIP | Five digits or ZIP+4 | First five digits |

Rule of thumb: if a value can't be understood, it becomes NULL. A NULL is honest, and the matching step treats it as no evidence.

Step B, match and link:
1. **Blocking:** only compare records that share the same date of birth. Comparing every record with every other record does not scale, and blocking keeps the comparison small.
2. **Rules:** for each candidate pair, decide whether it is the same person. The first rule that fires wins. The rules are ordered from strongest to weakest evidence:
   - same SSN
   - same phone and same last name
   - same email
   - same last name, first names within two typing edits, and same ZIP
3. **Clustering:** if A matches B and B matches C, then A, B, and C are one person. A connected-components algorithm (union-find) groups them.
4. **Entity ID:** each group gets an entity_id. A record with no match gets its own entity_id.

**Gold: survivorship (choose the best value)**

| Field | Rule |
|---|---|
| Name, date of birth, SSN, phone, email | The most frequent non-null value across the person's records; ties go to the most recently ingested |
| Address | The address from the most recently ingested record |
| Counts | How many records were merged, and from how many systems |
| Trace | The list of silver IDs and bronze IDs that formed the record |

Gold never invents data. If every record for a person lacked an email, the gold email stays empty.

### 3.4 Why Parquet

Each layer is stored as Parquet, a compressed, column-oriented format that keeps data types (for example, real dates). It is what real data lakes use for bronze, silver, and gold tables. The only CSV is the source extract that bronze is loaded from. CSV exports are available only for viewing.

### 3.5 Why PySpark (local mode)

- PySpark is what banks and data teams use at scale (Databricks, Fabric, EMR).
- The same code would run on a cluster with millions of rows.
- Running locally needs Java, because Spark's engine runs on the JVM and PySpark is a Python wrapper around it.
- At about 2,000 records pandas would be enough, but PySpark is the point of the exercise.

---

## 4. Implementation

### 4.1 Pipeline overview

| Step | File | Output |
|---|---|---|
| 1. Generate synthetic data | 01_generate_data.py | Source extract CSV and ground truth CSV |
| 2. Bronze load | 02_bronze.py | Bronze table (Parquet) |
| 3. Silver standardize | 03_silver.py | Standardized silver (Parquet, intermediate folder) |
| 4. Silver match | 04_silver_match.py | Final silver with entity_id and match_rule |
| 5. Gold | 05_gold.py | Gold table, one row per person |
| 6. Evaluate | 06_evaluate.py | Precision, recall, F1, and lists of errors |
| 7. Publish to Postgres and CSV | 07_publish.py | SQL tables and CSV copies for each layer (planned) |

### 4.2 The synthetic data

About 1,100 fictional people become roughly 2,000 source records across the three systems.

- A person appears in one system (40%), two systems (40%), or all three (20%).
- About 5% of records are copied exactly inside the same source, to create deduplication cases.
- The data is made messy on purpose:
  - four phone formats
  - three date formats
  - names as "First Last", uppercase, "Last, First", or with extra spaces
  - nicknames in place of formal names
  - occasional typos in first names
  - state as a code or a full name
  - ZIP as five digits or ZIP+4
  - abbreviated or uppercase street names
  - missing SSN (about 15%), phone (about 10%), and email (about 15%)
  - about 10% of records have a different street address, as if the person moved

**Ground truth:** a separate file records which person each record truly belongs to. It is used only for testing, never inside the pipeline. This is what allows accuracy to be measured.

### 4.3 Data layout on disk

- A raw folder holds the source extract CSV and the ground truth CSV.
- One folder each for bronze, silver, and gold, each holding Parquet files. Spark writes a table as a folder of part files plus a success marker.
- An intermediate folder holds standardized silver before matching. This is needed because Spark cannot overwrite a folder it is reading from.
- An exports folder holds CSV copies for viewing in Excel or an editor.

### 4.4 Planned database layer

After the pipeline is done, the same Parquet data is loaded into Postgres so the layers can be queried with SQL:

- One database with four schemas: bronze, silver, gold, and eval (for the ground truth).
- Primary keys on the three ID columns, and foreign keys from silver to bronze and from silver to gold, which makes the traceability design explicit.
- SQL practice topics: counts per layer, aggregation, joins that trace gold back to raw rows, data quality checks, window functions, and an SQL version of the precision and recall calculation.
- CSV files for each layer are produced from the same Parquet data, so the CSVs and SQL tables always match.

---

## 5. Challenges

**Environment challenges already met**
- **Java missing:** PySpark failed to start with a "Java gateway exited" error because no Java was installed. Spark runs on the JVM, so Java 17 is required, and its location had to be set for the shell.
- **Python version:** the newest Python on the machine was too new for PySpark, so a Python 3.12 virtual environment was used.
- **Ambiguous column error:** joining silver to bronze produced two columns with the same name, and Spark could not tell which was meant. The fix is to rename columns before a join.
- **Path mix-ups:** a later step could not find the intermediate folder because the earlier step had not been re-run with the new output location. Scripts must be run from the project root, and a step depends on the previous step's output existing.
- **Misleading file name:** the source extract was called "bronze_raw", which made it look like the bronze table. The CSV is the input, and the bronze table is the Parquet folder created from it.

**Data and matching challenges (the conceptual ones)**
- **Scale:** comparing every record with every other record is too slow on large data, which is why blocking is needed.
- **Missing data:** a missing SSN or phone must count as no evidence, not as a mismatch.
- **Typos survive standardization:** cleaning cannot fix a misspelled name, so a fuzzy comparison (edit distance) is needed.
- **Shared attributes:** a shared phone or address can belong to relatives, so a single weak field should not be enough to merge.
- **Chaining risk:** if A matches B and B matches C wrongly, A and C get merged. Weak rules can pull unrelated people into one group.
- **Choosing rules and thresholds:** looser rules find more matches but cause more wrong merges, and stricter rules do the opposite.
- **Blocking depends on clean fields:** blocking on date of birth works only because dates are reliable in this data. In real data a typo in the blocking field means a true match is never compared.

### Why precision and recall matter in banking

| Error | Meaning | Business cost |
|---|---|---|
| False negative (missed match) | The same person stays split | A missed sanctions hit or undetected structuring |
| False positive (wrong merge) | Two different people are combined | An innocent customer is flagged, or one person's data is exposed to another |

For sanctions screening, a bank prefers high recall (flag too many for review rather than miss one). For merging customer records, it prefers high precision (a wrong merge is hard to undo). So the two tasks would use different thresholds.

---

## 6. Testing

### 6.1 How accuracy is measured

Because the data is synthetic, the true answer is known. The pipeline's entity_id is compared to the true person ID, and the comparison is done on **pairs of records**.

| Result | Meaning |
|---|---|
| True positive | The pipeline grouped the pair together, and they really are the same person |
| False positive | The pipeline grouped the pair together, but they are different people |
| False negative | The pipeline kept the pair apart, but they are the same person |

- **Precision** = true positives divided by all pairs the pipeline linked. It answers: of the merges we made, how many were right?
- **Recall** = true positives divided by all pairs that truly match. It answers: of the real duplicates, how many did we find?
- **F1** is the balance of the two.
- Accuracy alone is a misleading metric here, because most pairs of records are not matches.

A group of n records contains n(n−1)/2 pairs, so the pair counts can be calculated from group sizes without building every pair.

### 6.2 Other checks along the way

- **Bronze:** the row count equals the source file, and the bronze ID is unique.
- **Silver (standardize):** every phone has ten digits or is NULL, and likewise for SSN (nine digits), ZIP (five digits), and state (two letters). Nothing is dropped, so silver has the same row count as bronze.
- **Silver (match):** the number of entities should be close to the number of real people. Fewer entities means wrong merges, and more means missed matches.
- **Gold:** the row count equals the entity count, and the gold ID is unique.
- **Trace test:** pick one gold record, follow its silver IDs and bronze IDs by hand, and explain why each chosen value won.
- **Error review:** list the wrongly merged groups and the wrongly split people, find the pattern, and decide which rule to change. This loop is the most valuable part of the exercise.

### 6.3 What to expect

The noise in this synthetic data is fairly mild, because dates of birth are never corrupted and ZIP codes stay consistent even when the street changes. Results may therefore look close to perfect. That does not prove the method is robust. To make testing more realistic, tighten the rules (for example, drop the ZIP condition) or add harder noise (typos in last names, wrong birth dates, name changes).

---

## 7. Current Status

| Item | Status |
|---|---|
| Business use case and design | Done |
| Environment (Java, Python 3.12, PySpark) | Done |
| Synthetic data generation | Done |
| Bronze load | Done |
| Silver standardization | Done |
| Silver matching (entity_id) | Run, results to be reviewed |
| Gold (survivorship) | In progress |
| Evaluation (precision, recall, F1) | In progress |
| Postgres load and SQL practice | Planned |
| CSV exports of each layer | Planned |
| Final write-up | This document, to be updated with results |

Update the numbers here after running the evaluation: records, entities found, real people, true positives, false positives, false negatives, precision, recall, F1.

---

## 8. Key Learnings

- **Medallion layers exist for a reason in entity resolution.** Raw data is kept so decisions can be re-run, and every decision is stored so it can be audited.
- **Standardize before you match.** Most missed matches in real projects come from formatting differences, not from clever data.
- **Never delete in silver.** Linking records by an ID keeps the evidence, and gold is where collapsing happens.
- **NULL is not a mismatch.** Missing data should add no evidence for or against a match.
- **Blocking is what makes matching feasible**, and its choice is a trade-off: it can silently hide true matches when the blocking field is dirty.
- **Precision and recall pull in opposite directions.** The right balance depends on the business cost of each error.
- **Ground truth makes improvement possible.** Without known answers you can't tell whether a rule change helped.
- **IDs on every layer give traceability.** A gold record can be explained back to the raw rows it came from.
- **Tooling lessons:** Spark needs Java, Parquet keeps types and is the standard lake format, and column name clashes in joins must be resolved before selecting.

---

## 9. Next Steps

**Immediate**
1. Finish the gold table and check the row count against the entity count.
2. Run the evaluation and record precision, recall, and F1.
3. Review the wrongly merged and wrongly split records, and tune the matching rules.
4. Update the Current Status section with real numbers.

**Database work**
5. Install Postgres, create the database and four schemas.
6. Load bronze, silver, gold, and ground truth into SQL tables and add keys.
7. Produce CSV copies of each layer.
8. Practice SQL: counts, joins that trace gold to bronze, data quality checks, window functions, and the precision and recall query.

**Later improvements**
- Add harder, more realistic noise to the data.
- Replace hard-coded rules with a probabilistic model that gives each pair a score, with three outcomes: automatic match, manual review, and no match.
- Add business entities and beneficial ownership (companies, trusts).
- Add household linkage, so relatives sharing an address are linked as a household without being merged as one person.
- Add watchlist screening against a sanctions list, favoring recall.
- Handle new records arriving later by matching them against existing entities instead of rebuilding everything.
- Use one bronze table per source system, as a real bank would, instead of one shared table.

---

## Glossary

| Term | Meaning |
|---|---|
| Medallion architecture | Organizing data into bronze (raw), silver (clean), and gold (business-ready) layers |
| Deduplication | Removing repeated copies of a record |
| Entity resolution | Deciding which records refer to the same real-world thing |
| Standardization | Converting different formats of the same value into one form |
| Blocking | Limiting comparisons to records that share a key field |
| Fuzzy matching | Comparing text allowing small differences, such as typos |
| Edit distance (Levenshtein) | The number of single-character changes needed to turn one string into another |
| Clustering | Grouping matched records so a chain of matches becomes one entity |
| Survivorship | The rules for choosing the best value when merged records disagree |
| Golden record | The single best record for a real entity |
| Ground truth | The known correct answer, used only for testing |
| Precision | Of the merges made, the share that were correct |
| Recall | Of the true duplicates, the share that were found |
| Parquet | A compressed, column-based file format that keeps data types |
| PySpark | The Python interface to Apache Spark |
