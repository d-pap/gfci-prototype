-- Migration to v2 schema - creating new tables alongside existing ones
-- This allows for dual-write during transition period
-- Date: 2025-06-23

/* ===== BRONZE SCHEMA - Raw Data Storage ===== */

-- 1) Bronze job calls - audit trail of every API call
create table if not exists bronze.job_calls (
    call_id         serial primary key,
    run_date        date not null,                    -- The DAY of the run (2025-06-25)
    city            text not null,                    -- City of the search ("Detroit, MI")
    role            text not null,                    -- Search term used ("data analyst")
    pages_fetched   int not null,                     -- Adzuna number of pages fetched
    api_count       int,                              -- Total matching jobs from Adzuna `count` field
    api_mean_salary numeric,                          -- Mean salary from Adzuna `mean` field
    pulled_at       timestamptz default current_timestamp, -- Exact call timestamp
    jobs_retrieved  int not null,                     -- Adzuna number of jobs retrieved
);

-- Indexes for bronze.job_calls
create index if not exists idx_job_calls_run_date on bronze.job_calls(run_date);
create index if not exists idx_job_calls_city_role on bronze.job_calls(city, role);

-- 2) Bronze raw jobs - one row per unique job_id with update tracking
create table if not exists bronze.raw_jobs (
    job_id          text not null,                    -- Adzuna job ID
    source          text not null default 'adzuna',   -- Data source (future-proofing)
    first_seen      date not null,                    -- First time we saw this job
    last_seen       date not null,                    -- Most recent time we saw this job
    times_seen      int default 1,                    -- How many times we've seen this job
    payload         jsonb not null,                   -- Complete raw JSON from API
    created_at      timestamptz default current_timestamp,
    updated_at      timestamptz,
    
    primary key (source, job_id)
);

-- Indexes for bronze.raw_jobs
create index if not exists idx_raw_jobs_last_seen on bronze.raw_jobs(last_seen);
create index if not exists idx_raw_jobs_first_seen on bronze.raw_jobs(first_seen);

-- Trigger to update updated_at timestamp
create or replace function update_updated_at_column()
returns trigger as $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

create trigger update_raw_jobs_updated_at before update
    on bronze.raw_jobs for each row execute function update_updated_at_column();

/* ===== SILVER SCHEMA - Cleaned and Enriched Data ===== */

-- Silver jobs - parsed and enriched job data (no description to save space)
create table if not exists silver.jobs_v2 (
    -- Identity
    source          text not null default 'adzuna',
    job_id          text not null,
    
    -- Basic fields from API
    title           text not null,
    company         text,
    location        text,
    city            text,
    county          text,
    state           text,
    state_code      text,                             -- Two-letter state code
    cbsa_code       text,                             -- Metropolitan area code
    
    -- Categories and classification
    category        text,                             -- Original category from API
    category_label  text,                             -- Human-readable category
    
    -- Compensation
    salary_min      numeric,
    salary_max      numeric,
    
    -- Dates and tracking
    post_date       date,                             -- When job was posted
    first_seen      date not null,                    -- When we first saw it
    last_seen       date not null,                    -- When we last saw it
    times_seen      int default 1,
    
    -- Status
    is_active       boolean default true,             -- TRUE if seen in today's API call
    
    -- URL
    url             text,
    
    -- Coordinates
    latitude        numeric,
    longitude       numeric,
    
    -- Derived fields
    seniority       text check (seniority in ('jr', 'mid', 'sr')),
    is_remote       boolean default false,
    industry        text,
    job_type        text,                             -- full-time, part-time, contract, etc.
    yoe_min         int,                          -- Years of experience minimum
    education       text,                             -- Education requirement
    
    -- Metadata
    processed_at    timestamptz default current_timestamp,
    updated_at      timestamptz default current_timestamp,
    
    primary key (source, job_id)
);

-- Indexes for silver.jobs_v2
create index if not exists idx_jobs_v2_city_state on silver.jobs_v2(city, state);
create index if not exists idx_jobs_v2_is_active on silver.jobs_v2(is_active);
create index if not exists idx_jobs_v2_last_seen on silver.jobs_v2(last_seen);

-- Trigger for silver.jobs_v2 updated_at
create trigger update_jobs_v2_updated_at before update
    on silver.jobs_v2 for each row execute function update_updated_at_column();

/* ===== GOLD SCHEMA - Analysis-Ready Tables ===== */

-- City job statistics - aggregated metrics per city
create table if not exists gold.city_job_stats (
    city            text not null,
    state           text not null,
    run_date        date not null,
    
    -- Job counts
    total_jobs      int not null,                 -- Total unique jobs ever seen
    active_jobs     int not null,                 -- Jobs active as of run_date
    new_jobs        int not null,                 -- Jobs first seen on run_date
    expired_jobs    int not null,                 -- Jobs that disappeared
    
    -- Salary metrics
    jobs_with_salary int,                         -- Count of jobs with salary data
    avg_salary_min  numeric,
    avg_salary_max  numeric,
    median_salary_min numeric,
    median_salary_max numeric,
    
    -- Job type breakdowns
    remote_jobs     int default 0,
    fulltime_jobs   int default 0,
    
    -- Seniority breakdown
    junior_jobs     int default 0,
    mid_jobs        int default 0,
    senior_jobs     int default 0,
    
    -- Metadata
    created_at      timestamptz default current_timestamp,
    
    primary key (city, state, run_date)
);

-- Index for gold.city_job_stats
create index if not exists idx_city_job_stats_run_date on gold.city_job_stats(run_date);

-- Latest city snapshot - most recent data for dashboards
create table if not exists gold.latest_city_snapshot (
    city            text not null,
    state           text not null,
    
    -- Job market data
    active_jobs     int not null,
    new_jobs_7d     int,                          -- New jobs in last 7 days
    new_jobs_30d    int,                          -- New jobs in last 30 days
    avg_salary      numeric,
    job_growth_rate_7d numeric,                       -- Percentage growth last 7 days
    job_growth_rate_30d numeric,                      -- Percentage growth last 30 days
    
    -- Housing data (from silver.housing_metrics)
    zori_latest     numeric,                          -- Latest rent index
    zori_date       date,                             -- Date of ZORI data
    
    -- Computed metrics
    salary_to_rent_ratio numeric,                     -- Avg salary / ZORI
    
    -- Top categories in this city
    top_categories  text[],                           -- Array of top 5 job categories
    
    -- Metadata
    last_updated    timestamptz default current_timestamp,
    
    primary key (city, state)
);

-- Create indexes for common query patterns
create index if not exists idx_latest_snapshot_jobs on gold.latest_city_snapshot(active_jobs DESC);
create index if not exists idx_latest_snapshot_salary on gold.latest_city_snapshot(avg_salary DESC);

-- View to help with city name standardization between jobs and housing data
create or replace view gold.city_mapping as
select distinct
    j.city,
    j.state,
    j.state_code,
    h.region_name,
    h.cbsa_code
from silver.jobs_v2 j
left join silver.housing_metrics h 
    on lower(j.city) = lower(h.region_name) 
    and j.state_code = h.state_code
where j.is_active = TRUE; 