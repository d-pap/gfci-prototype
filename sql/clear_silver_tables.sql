-- FOR DEVELOPMENT ONLY: clear processed data but keep raw data
TRUNCATE TABLE silver.jobs CASCADE;
TRUNCATE TABLE silver.housing_metrics CASCADE;
-- keep bronze.adzuna_jobs, bronze.jsearch_jobs, bronze.zillow_zori, etc. 