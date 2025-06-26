-- creates all your tables in `bronze`, `silver`, and `gold` schemas
-- run once at the start or when adding new tables

-- BRONZE SCHEMA: one table per source, keep as-is
-- SILVER SCHEMA: one table per DOMAIN (e.g., jobs, rents, etc.), standardize and combine where it makes sense
-- GOLD SCHEMA: final answers, joined and aggregated across domains (e.g., city job score, job posting trends, etc.)


/* ----- BRONZE TABLES WITH RAW DATA ----- */
create table if not exists bronze.adzuna_jobs (
    -- raw job data from adzuna api

    id serial primary key, -- internal row id 
    source text not null, -- adzuna, jsearch, etc.
    job_id text not null, -- api job id if available
    fetched_at timestamp default current_timestamp, -- pull date
    payload JSONB not null, -- raw job data from API

    unique (job_id, fetched_at) -- allow one record per day
);

create table if not exists bronze.jsearch_jobs (
    -- raw job data from jsearch api

    id serial primary key,
    source text not null, -- jsearch, etc.
    job_id text not null, -- api job id if available
    fetched_at timestamp default current_timestamp, -- pull date
    payload JSONB not null, -- raw job data from API

    unique (job_id, fetched_at) -- allow one record per day
);

-- bronze.zillow_zori - created by pandas with all CSV columns 
-- ! therefore these columns are not used for this table, they get overwritten
create table if not exists bronze.zillow_zori (
    -- raw rent data from zillow zori csv

    id serial primary key,
    cbsa_code text,
    region_id integer,
    region_name text,
    state_code text,
    size_rank integer,
    zori_latest numeric,
    inserted_at timestamp default current_timestamp
);

-- add other tables here

/* ----- SILVER TABLES WITH CLEANED/ENRICHED DATA ----- */

create table if not exists silver.jobs (
    -- combine adzuna and jsearch jobs, add fields like seniority, industry, job_type, yoe_min, education, etc.

    source text not null, 
    job_id text not null,

    -- raw fields
    title text,
    company text,
    location text,
    city text,
    county text,
    state text, 
    cbsa_code text,
    category text,
    description text,
    post_date date, 
    url text,

    salary_min numeric,
    salary_max numeric,

    -- derived fields
    seniority text check (seniority in ('jr', 'mid', 'sr')),
    is_remote boolean,
    industry text,
    job_type text, -- full-time, part-time, contract, internship, etc.
    yoe_min integer,
    education text, -- bachelor's, master's, phd, etc.

    -- metadata
    processed_at timestamp default current_timestamp,

    primary key (source, job_id)
);

-- create table if not exists silver.zori_rents (
--     -- cleaned column types, city mapped to cbsa code

--     id serial primary key,
--     cbsa_code text,
--     region_id integer,
--     region_name text,
--     state_code text,
--     size_rank integer,
--     zori_latest numeric,
--     inserted_at timestamp default current_timestamp
-- );

-- SILVER: standardized housing data from zillow CSVs
create table if not exists silver.housing_metrics (
    id serial primary key,
    region_id integer,
    region_name text,
    state_code text,
    size_rank integer,
    cbsa_code text,

    -- standardized metrics
    data_source text not null, -- "zillow_zori", "zillow_zhvi", etc.
    metric_type text not null, -- "rent", "home_price", "income_needed", etc.
    metric_value_latest numeric,
    date_recorded text, -- original date from CSV

    -- metadata
    processed_at timestamp default current_timestamp
);

/* ----- GOLD TABLES WITH AGGREGATED/DERIVED DATA ----- */
-- these tables join across sources (jobs + rent), aggregate data by city or category, and answer specific business questions
    -- # Join silver.jobs + silver.housing_metrics
    -- # Create gold.city_rankings, gold.market_trends, etc.

-- gold.city_job_score
-- ------------------------
-- city
-- total_jobs
-- avg_salary
-- remote_job_share
-- rent_index
-- score

-- gold.job_posting_trends
-- ------------------------
-- date
-- city
-- source
-- job_count
-- avg_salary


-- -- Summary table for your analysis
-- CREATE TABLE silver.city_job_market_metrics (
--     city TEXT,
--     state TEXT,
--     date DATE,
    
--     -- Volume metrics
--     total_active_jobs INTEGER,
--     new_jobs_today INTEGER,
    
--     -- Quality metrics  
--     avg_job_persistence_days NUMERIC,
--     jobs_with_salary_data INTEGER,
    
--     -- Trend metrics
--     week_over_week_change NUMERIC,
--     month_over_month_change NUMERIC,
    
--     PRIMARY KEY (city, state, date)
-- );