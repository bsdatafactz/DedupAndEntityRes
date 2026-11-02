ALTER TABLE bronze.customers ADD PRIMARY KEY (bronze_id);
ALTER TABLE gold.customers   ADD PRIMARY KEY (gold_id);
ALTER TABLE gold.customers   ADD CONSTRAINT uq_gold_entity UNIQUE (entity_id);
ALTER TABLE silver.customers ADD PRIMARY KEY (silver_id);

ALTER TABLE silver.customers ADD CONSTRAINT fk_silver_bronze
  FOREIGN KEY (bronze_id) REFERENCES bronze.customers (bronze_id);
ALTER TABLE silver.customers ADD CONSTRAINT fk_silver_gold
  FOREIGN KEY (entity_id) REFERENCES gold.customers (entity_id);