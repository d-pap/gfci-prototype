-- Sample queries for v2 schema

-- 1. Top cities by active jobs
SELECT 
    city, 
    state, 
    active_jobs,
    avg_salary,
    new_jobs_7d,
    job_growth_rate_7d
FROM gold.latest_city_snapshot
ORDER BY active_jobs DESC
LIMIT 10;

-- 2. Job market trends for a specific city
SELECT 
    run_date,
    total_jobs,
    active_jobs,
    new_jobs,
    avg_salary_min,
    avg_salary_max
FROM gold.city_job_stats
WHERE city = 'Chicago' AND state = 'IL'
ORDER BY run_date DESC
LIMIT 30;

-- 3. Cities with highest remote job percentage
SELECT 
    city,
    state,
    active_jobs,
    remote_jobs,
    ROUND(100.0 * remote_jobs / NULLIF(active_jobs, 0), 2) as remote_percentage
FROM gold.city_job_stats
WHERE run_date = CURRENT_DATE
ORDER BY remote_percentage DESC
LIMIT 10;

-- 4. Seniority distribution by city
SELECT 
    city,
    state,
    junior_jobs,
    mid_jobs,
    senior_jobs,
    ROUND(100.0 * junior_jobs / NULLIF(active_jobs, 0), 2) as junior_pct,
    ROUND(100.0 * mid_jobs / NULLIF(active_jobs, 0), 2) as mid_pct,
    ROUND(100.0 * senior_jobs / NULLIF(active_jobs, 0), 2) as senior_pct
FROM gold.city_job_stats
WHERE run_date = CURRENT_DATE
ORDER BY active_jobs DESC;

-- 5. Job persistence analysis
SELECT 
    city,
    state_code,
    COUNT(*) as total_jobs,
    AVG(times_seen) as avg_times_seen,
    COUNT(*) FILTER (WHERE times_seen = 1) as single_day_jobs,
    COUNT(*) FILTER (WHERE times_seen > 7) as persistent_jobs
FROM silver.jobs_v2
WHERE source = 'adzuna'
GROUP BY city, state_code
ORDER BY total_jobs DESC;

-- 6. Salary ranges by city
SELECT 
    city,
    state,
    jobs_with_salary,
    ROUND(avg_salary_min) as avg_min_salary,
    ROUND(avg_salary_max) as avg_max_salary,
    ROUND(median_salary_min) as median_min_salary,
    ROUND(median_salary_max) as median_max_salary
FROM gold.city_job_stats
WHERE run_date = CURRENT_DATE
    AND jobs_with_salary > 10
ORDER BY avg_salary_max DESC; 