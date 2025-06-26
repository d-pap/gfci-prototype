# Migration Plan to V2 Schema

## Overview

This migration redesigns the database schema to better support job market analysis with proper separation of concerns across bronze, silver, and gold schemas.

## Schema Design

### Bronze Schema (Raw Data)

1. **bronze.job_calls** - Audit trail of API calls

   - One row per API call
   - Tracks: run_date, city, role, page, api_count, api_mean_salary
   - Purpose: Market size signals and API audit trail

2. **bronze.raw_jobs** - Unique jobs with lifecycle tracking
   - One row per unique job_id
   - Updates: last_seen date and times_seen counter
   - Stores complete JSON payload
   - Purpose: Historical job tracking without duplication

### Silver Schema (Cleaned Data)

1. **silver.jobs_v2** - Parsed and enriched job data
   - All fields extracted from JSON into columns
   - Derived fields: seniority, industry, is_remote, etc.
   - is_active flag (true if seen today)
   - NO description field (saves space)
   - Includes first_seen and last_seen dates

### Gold Schema (Analysis-Ready)

1. **gold.city_job_stats** - Daily aggregated metrics per city

   - Job counts: total, active, new, expired
   - Salary statistics
   - Seniority and job type breakdowns

2. **gold.latest_city_snapshot** - Current market state
   - Latest job counts and trends
   - ZORI housing data integration
   - Salary-to-rent ratios
   - For dashboards and quick lookups

## Migration Steps

### Phase 1: Setup (Current)

1. Run migration script to create new tables:

   ```bash
   python scripts/migrate_to_v2.py
   ```

2. This creates all v2 tables and tests basic functionality

### Phase 2: Dual-Write (Testing)

1. Use `ingest_adzuna_v2.py` which writes to both schemas
2. Update main.py to use the v2 ingest function:

   ```python
   from ingest.ingest_adzuna_v2 import ingest_adzuna
   ```

3. Test with small data sets to verify dual-write works

### Phase 3: Create Enrichment Functions

1. Create `transform/enrich_adzuna_v2.py` for silver.jobs_v2
2. Create aggregation functions for gold schema tables
3. Test the full pipeline: bronze → silver → gold

### Phase 4: Cutover

1. Once stable, stop writing to old schema
2. Update all queries to use new tables
3. Eventually drop old tables

## Key Benefits

1. **No duplicate jobs in bronze** - Uses upsert logic
2. **Market trend tracking** - job_calls table provides insights
3. **Efficient storage** - One row per job, updated in place
4. **Analysis-ready gold tables** - Pre-aggregated for performance
5. **Backward compatible** - Dual-write during migration

## Example Queries

### Find cities with most active jobs:

```sql
SELECT city, state, active_jobs
FROM gold.latest_city_snapshot
ORDER BY active_jobs DESC
LIMIT 10;
```

### Track job market trends:

```sql
SELECT run_date, city, total_jobs, new_jobs
FROM gold.city_job_stats
WHERE city = 'Chicago'
ORDER BY run_date DESC
LIMIT 30;
```

### Analyze job persistence:

```sql
SELECT
    city,
    AVG(times_seen) as avg_times_seen,
    AVG(last_seen - first_seen) as avg_days_active
FROM silver.jobs_v2
WHERE is_active = true
GROUP BY city;
```
