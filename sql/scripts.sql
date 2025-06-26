--------------------------------------------------------------------------------
-- add unique constraint for one row per city/role/day to bronze.job_calls
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- -- See what the duplicates look like
-- SELECT run_date, city, role, pages_fetched, jobs_retrieved, pulled_at
-- FROM bronze.job_calls
-- WHERE (run_date, city, role) IN (
--     SELECT run_date, city, role
--     FROM bronze.job_calls
--     GROUP BY run_date, city, role
--     HAVING COUNT(*) > 1
-- )
-- ORDER BY run_date, city, role, pulled_at;

-- -- Delete all but the most recent row for each city/role/date
-- DELETE FROM bronze.job_calls
-- WHERE call_id NOT IN (
--     SELECT DISTINCT ON (run_date, city, role) call_id
--     FROM bronze.job_calls
--     ORDER BY run_date, city, role, pulled_at DESC
-- );

-- -- Now add the unique constraint
-- ALTER TABLE bronze.job_calls 
-- ADD CONSTRAINT unique_job_call_per_day 
-- UNIQUE (run_date, city, role);
--------------------------------------------------------------------------------
-- add description column to silver.jobs_v2 
--------------------------------------------------------------------------------

-- -- add description column
-- ALTER TABLE silver.jobs_v2 
-- ADD COLUMN description text;

-- select * from silver.jobs_v2 limit 10;

