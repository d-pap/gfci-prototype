-- put enrichment that isnt logic-heavy here
-- if logic-heavy, put it in utils.py and use python to do it

--! use for:
-- 1. joins with other tables (e.g., cbsa_lookup)
-- 2. column cleanup (e.g., normalize categories)
-- 3. bulk updates when logic is simple and deterministic (e.g., updating cbsa_code)
--! use python (util.py) for:
-- text analysis (e.g., get_is_remote, get_industry, get_job_type, get_yoe, get_education)
-- anything involving pattern matching, NLP, or logic branching e.g. categorize_seniority

-- Update cbsa_code
-- UPDATE jobs_clean 
-- SET cbsa_code = cbsa_lookup.cbsa_code
-- FROM cbsa_lookup
-- WHERE 
--     LOWER(TRIM(jobs_clean.state)) = LOWER(TRIM(cbsa_lookup.state_code))
--     AND LOWER(TRIM(jobs_clean.city)) = LOWER(TRIM(cbsa_lookup.primary_city));


-- Industry classification
-- UPDATE jobs_clean 
-- SET industry = CASE
--     WHEN company ~* '\b(google|microsoft|amazon|apple|meta|netflix|tesla|uber|airbnb)\b' THEN 'big_tech'
--     WHEN company ~* '\b(jpmorgan|goldman|morgan stanley|wells fargo|bank of america|citi)\b' THEN 'finance'
--     WHEN company ~* '\b(mckinsey|bain|bcg|deloitte|pwc|accenture|kpmg)\b' THEN 'consulting'
--     WHEN company ~* '\b(pfizer|merck|johnson|roche|novartis|astrazeneca)\b' THEN 'pharma'
--     WHEN title ~* '\bfintech\b' OR company ~* '\b(stripe|square|robinhood|coinbase)\b' THEN 'fintech'
--     WHEN title ~* '\bhealthcare?\b' OR company ~* '\b(health|medical|hospital)\b' THEN 'healthcare'
--     WHEN company ~* '\b(startup|inc\.?|llc)\b' AND 
--          (SELECT COUNT(*) FROM jobs_clean WHERE company = jobs_clean.company) < 10 THEN 'startup'
--     ELSE 'other'
-- END
-- WHERE industry IS NULL;


-- Update processing timestamp for tracking 
UPDATE jobs_clean 
SET processed_at = CURRENT_TIMESTAMP 
WHERE processed_at < (CURRENT_TIMESTAMP - INTERVAL '1 hour');

--! add cbsa_code to zillow_zori
-- UPDATE zillow_zori z 
-- SET cbsa_cose = c.cbsa_code
-- FROM cbsa_code c
-- WHERE