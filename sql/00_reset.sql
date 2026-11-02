-- Full refresh: drop and recreate the four schemas so 07_publish.py can reload cleanly.
-- Dropping with CASCADE also removes the primary and foreign keys; 01_keys.sql adds them back.
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold   CASCADE;
DROP SCHEMA IF EXISTS eval   CASCADE;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
CREATE SCHEMA eval;
