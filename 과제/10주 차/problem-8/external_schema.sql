-- Problem 8: External Schema Definition for Redshift Spectrum
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_raw
FROM DATA CATALOG
DATABASE 'spectrum_db'
IAM_ROLE 'arn:aws:iam::268790346668:role/redshift-copy-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
