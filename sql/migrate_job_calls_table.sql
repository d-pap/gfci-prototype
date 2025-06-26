-- Migration: Consolidate job_calls table
-- Date: 2025-06-26
-- Description: Rename page to pages_fetched, add jobs_retrieved, consolidate rows to only one row per API call (instead of one row per page of API response)

-- STEP 1: explore the table to understand the data and the structure
-- select * from bronze.job_calls;

-- STEP 2: change "page" column name to "pages_fetched"
-- alter table bronze.job_calls rename column page to pages_fetched;

-- STEP 3: add "jobs_retrieved" column
-- alter table bronze.job_calls add column jobs_retrieved integer;

-- STEP 4: create a backup table
-- create table bronze.job_calls_backup as
-- select * from bronze.job_calls;

-- select * from bronze.job_calls;
-- select * from bronze.job_calls_backup;

-- STEP 5: consolidate rows
-- select city, role, run_date, count(*) as pages_fetched, max(api_count) as api_count, max(api_mean_salary) as api_mean_salary, min(pulled_at) as pulled_at, max(jobs_retrieved) as jobs_retrieved
-- from bronze.job_calls
-- group by city, role, run_date
-- order by pages_fetched desc;

-- STEP 6: insert the consolidated rows into the new table
-- -- 6.1 clear the table bronze.job_calls
-- truncate table bronze.job_calls;
-- select * from bronze.job_calls;

-- -- 6.2 insert into new table
-- insert into bronze.job_calls (city, role, run_date, pages_fetched, api_count, api_mean_salary, pulled_at, jobs_retrieved)
-- select city, role, run_date, count(*) as pages_fetched, max(api_count) as api_count, max(api_mean_salary) as api_mean_salary, min(pulled_at) as pulled_at, max(jobs_retrieved) as jobs_retrieved
-- from bronze.job_calls_backup
-- group by city, role, run_date
-- order by pages_fetched desc;

-- STEP 7: cleanup and drop the backup table
-- drop table bronze.job_calls_backup;
-- select * from bronze.job_calls_backup;
-- select * from bronze.job_calls;