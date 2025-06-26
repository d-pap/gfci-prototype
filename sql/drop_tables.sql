-- drop all tables

-- use to clean up everything so you can rebuild from scratch
-- useful when schemas or table definitions change

-- call it: run_sql_file("sql/drop_tables.sql")

drop table if exists bronze.adzuna_jobs;
drop table if exists bronze.jsearch_jobs;
drop table if exists bronze.zillow_zori;
drop table if exists silver.jobs;
drop table if exists silver.rents;
-- drop table if exists gold.city_job_score;
-- drop table if exists gold.job_posting_trends;