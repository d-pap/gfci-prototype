-- insert test data to test transforms, enrichments, and queries without running full API pulls 
-- call it: run_sql_file("sql/test_data.sql")

-- useful for:
    -- to test transform logic (like enrich_adzuna.py) with a few rows and dont want to pull from api
    -- to test joins with other tables (e.g., cbsa_lookup)
    -- to test bulk updates when logic is simple and deterministic (e.g., updating cbsa_code)


INSERT INTO bronze.adzuna_jobs (job_id, fetched_at, raw_json)
VALUES 
('adz-123', '2024-01-01', '{"title": "Data Analyst", "location": "Chicago, IL", "salary_min": 60000, "salary_max": 80000}'::jsonb),
('adz-456', '2024-01-01', '{"title": "Junior Data Scientist", "location": "Detroit, MI", "salary_min": 55000, "salary_max": 75000}'::jsonb);

INSERT INTO bronze.zillow_zori (region_name, state, cbsa_code, date, value)
VALUES 
('Chicago', 'IL', '16974', '2024-01-01', 1400),
('Detroit', 'MI', '19820', '2024-01-01', 1050);
