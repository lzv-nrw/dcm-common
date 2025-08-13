-- example comment containing a semicolon; mid comment

BEGIN TRANSACTION;
CREATE TABLE sample_table (
  id uuid PRIMARY KEY
);
CREATE TABLE sample_table2 (
  id text PRIMARY KEY -- example comment within statement
);
INSERT INTO sample_table2 VALUES ('value containing semicolon; mid value');
COMMIT;
