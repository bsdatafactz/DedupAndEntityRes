SELECT * FROM bronze.customers LIMIT 10;
SELECT * FROM silver.customers LIMIT 10;
SELECT * FROM gold.customers   LIMIT 10;

-- 2. Rows per layer
SELECT 'bronze' AS layer, COUNT(*) FROM bronze.customers
UNION ALL SELECT 'silver', COUNT(*) FROM silver.customers
UNION ALL SELECT 'gold',   COUNT(*) FROM gold.customers;

-- 3. Records per source system
SELECT source_system, COUNT(*) FROM bronze.customers GROUP BY source_system;

-- 4. How many people appear in 1, 2, 3 systems?
SELECT source_count, COUNT(*) AS people FROM gold.customers
GROUP BY source_count ORDER BY source_count;

-- 5. Which rule linked the most records?
SELECT match_rule, COUNT(*) FROM silver.customers GROUP BY match_rule ORDER BY 2 DESC;

-- 6. Trace gold back to the raw messy rows (the main exercise)
SELECT g.gold_id, s.silver_id, b.bronze_id, b.source_system,
       b.full_name AS raw_name, s.first_name, s.last_name,
       b.phone AS raw_phone, s.phone
FROM gold.customers g
JOIN silver.customers s ON s.entity_id = g.entity_id
JOIN bronze.customers b ON b.bronze_id = s.bronze_id
WHERE g.record_count >= 3
ORDER BY g.gold_id, s.silver_id
LIMIT 20;

-- 7. How messy were the bronze phone numbers?
SELECT CASE WHEN phone ~ '^\(\d{3}\) \d{3}-\d{4}$' THEN '(555) 014-2000'
            WHEN phone ~ '^\d{3}-\d{3}-\d{4}$'     THEN '555-014-2000'
            WHEN phone ~ '^\+1'                    THEN '+1 ...'
            WHEN phone ~ '^\d{10}$'                THEN 'digits only'
            WHEN phone IS NULL                     THEN 'NULL'
            ELSE 'other' END AS format,
       COUNT(*)
FROM bronze.customers GROUP BY 1 ORDER BY 2 DESC;

-- 8. Missed matches: the same SSN in more than one gold row
SELECT ssn, COUNT(*) FROM gold.customers
WHERE ssn IS NOT NULL GROUP BY ssn HAVING COUNT(*) > 1;

-- 9. Latest record per entity (what survivorship did in Spark)
SELECT * FROM (
  SELECT s.*, ROW_NUMBER() OVER (PARTITION BY entity_id
                                 ORDER BY ingested_at DESC, silver_id DESC) AS rn
  FROM silver.customers s) t
WHERE rn = 1 LIMIT 10;

-- 10. Explode the comma list in gold back into rows
SELECT gold_id, UNNEST(STRING_TO_ARRAY(silver_ids, ',')) AS silver_id
FROM gold.customers LIMIT 10;

-- 11. Precision and recall in SQL (same logic as 06_evaluate.py)
WITH labelled AS (
  SELECT s.silver_id, s.entity_id, t.true_person_id
  FROM silver.customers s JOIN eval.ground_truth t USING (bronze_id)
),
pairs AS (
  SELECT (a.entity_id = b.entity_id)           AS predicted_same,
         (a.true_person_id = b.true_person_id) AS actually_same
  FROM labelled a JOIN labelled b ON a.silver_id < b.silver_id
)
SELECT COUNT(*) FILTER (WHERE predicted_same AND actually_same)     AS true_pos,
       COUNT(*) FILTER (WHERE predicted_same AND NOT actually_same) AS false_pos,
       COUNT(*) FILTER (WHERE NOT predicted_same AND actually_same) AS false_neg
FROM pairs;