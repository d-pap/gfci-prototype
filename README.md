# Graduate Job Market Analysis

A data pipeline for analyzing job markets across major U.S. cities to help new college graduates make informed decisions about where to start their data careers.

## Project Overview

This project collects and analyzes job market data from multiple sources to provide insights into:

- Entry-level job availability by city
- Salary ranges for new graduates
- Cost of living vs. salary ratios
- Remote work opportunities
- Industry trends and company insights

## Data Sources

- **Adzuna API**: Job postings data
- **JSearch API** (RapidAPI): Additional job postings
- **Zillow CSV**: Housing costs and rental prices
- **BLS Data**: Economic indicators (planned)

## Project Structure

```
gfci/
├── main.py                    # Main pipeline orchestrator
├── requirements.txt           # Python dependencies
├── .env                      # Environment variables (create this)
├── README.md                 # This file
│
├── src/                      # Source code modules
│   ├── __init__.py
│   ├── database/
│   │   └── connection.py     # Database connection utilities
│   ├── scrapers/
│   │   ├── base.py          # Base scraper class
│   │   ├── adzuna.py        # Adzuna API scraper
│   │   └── jsearch.py       # JSearch API scraper
│   └── loaders/
│       └── housing_data.py   # Housing data CSV loader
│
├── sql/                      # Database schema and queries
│   ├── 01_init_schema.sql   # Initial schema setup
│   └── 02_sql_enrichment.sql # Data enrichment pipeline
│
├── data/                     # Data files (create this)
│   └── housing/             # Housing CSV files
│
├── scripts/                  # Legacy scripts (can be deprecated)
│   ├── fetch_adzuna.py      # Old Adzuna scraper
│   └── fetch_jsearch.py     # Old JSearch scraper
│
└── notebooks/               # Jupyter notebooks for analysis
    └── analysis.ipynb       # Main analysis notebook
```

## Setup Instructions

### 1. Environment Setup

```bash
# Create virtual environment
python -m venv gfci-venv
source gfci-venv/bin/activate  # On Windows: gfci-venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Setup

Make sure PostgreSQL is running in Docker:

```bash
docker run --name gfci-postgres -e POSTGRES_PASSWORD=yourpassword -p 5432:5432 -d postgres
```

### 3. Environment Variables

Create a `.env` file in the project root:

```bash
# Database connection
PG_USER=postgres
PG_PASSWORD=yourpassword
PG_HOST=localhost
PG_PORT=5432
PG_DB=gfci

# API Keys
ADZUNA_APP_ID=your_adzuna_app_id
ADZUNA_APP_KEY=your_adzuna_api_key
RAPIDAPI_KEY=your_rapidapi_key
```

### 4. Initialize Database

```bash
python main.py --init-db
```

## Usage

### Data Collection

```bash
# Collect job data from all sources
python main.py --collect-jobs

# Load housing data from CSV files
python main.py --load-housing

# Run data enrichment pipeline
python main.py --run-enrichment

# Complete refresh (all steps)
python main.py --full-refresh
```

### Analysis

```bash
# Generate basic analysis report
python main.py --analyze
```

## Database Schema

### Core Tables

- **`jobs_raw`**: Raw job data from APIs (immutable)
- **`jobs_enriched`**: Processed and enriched job data for analysis
- **`metro_areas`**: Metropolitan area reference data
- **`city_economics`**: Housing costs and economic indicators

### Key Views

- **`job_market_summary`**: Aggregated job market statistics by city

## Key Features

### 1. Multi-Source Data Integration

- Modular scraper architecture for easy addition of new data sources
- Consistent data normalization across different APIs
- Automatic deduplication and data quality scoring

### 2. Robust Data Pipeline

- Separation of raw and processed data
- Idempotent operations (safe to re-run)
- Error handling and logging
- Rate limiting for API calls

### 3. Graduate-Focused Analysis

- Seniority level classification
- Entry-level job identification
- Salary-to-cost-of-living ratios
- Remote work opportunity tracking

## Analysis Capabilities

The pipeline provides insights into:

1. **Entry-Level Job Market**: Percentage of jobs suitable for new graduates
2. **Geographic Opportunities**: Job availability by metro area
3. **Salary Expectations**: Compensation ranges for different roles
4. **Cost of Living**: Housing costs vs. salary ratios
5. **Remote Work**: Percentage of remote-friendly positions
6. **Industry Trends**: Top hiring companies and sectors

## Extending the Project

### Adding New Data Sources

1. Create a new scraper class inheriting from `BaseScraper`
2. Implement the required methods: `fetch_jobs()` and `normalize_job_data()`
3. Add the scraper to the main pipeline in `main.py`

### Adding New Analysis

1. Create SQL queries in the `sql/` directory
2. Add analysis functions to `main.py`
3. Create Jupyter notebooks for detailed exploration

## Best Practices Implemented

1. **Separation of Concerns**: Raw data vs. processed data
2. **Modular Architecture**: Easy to extend and maintain
3. **Data Quality**: Scoring and validation
4. **Error Handling**: Graceful failure recovery
5. **Documentation**: Clear code and usage instructions
6. **Reproducibility**: Consistent data pipeline

## Future Enhancements

- [ ] Add BLS economic data integration
- [ ] Implement data validation with Pydantic
- [ ] Add automated scheduling with Airflow/Cron
- [ ] Create web dashboard for real-time insights
- [ ] Add machine learning for salary prediction
- [ ] Implement data lineage tracking

## Contributing

1. Follow the existing code structure
2. Add tests for new functionality
3. Update documentation
4. Use meaningful commit messages

## License

This project is for educational and personal use.

---

---

**User**

im asking more about stuff specific to the bronze schema's jobs tables, based on what im trying to do, should i not store the same job if it is posted on multiple days? should i store that job that is already in my db but just on a different day but add a `is_new_posting` column? should i do something else? help me figure this out

---

**Cursor**

Excellent question! This is a crucial design decision that will impact all your analysis. Let me break down the options and recommend the best approach for your specific use case.

## **🤔 The Core Decision**

You have the same job appearing on multiple days. Your options:

### **Option 1: Keep All Records (Current Approach)**

```sql
-- Same job_id appears multiple times with different fetched_at
job_id='12345', fetched_at='2025-06-20', payload={...}
job_id='12345', fetched_at='2025-06-21', payload={...}
job_id='12345', fetched_at='2025-06-22', payload={...}
```

### **Option 2: Single Record + Update Metadata**

```sql
-- One record per job_id, update metadata when seen again
job_id='12345', first_seen='2025-06-20', last_seen='2025-06-22', days_active=3, payload={...}
```

### **Option 3: Hybrid Approach (Recommended)**

```sql
-- Keep one "master" record + track sightings separately
-- Table 1: bronze.job_postings (unique jobs)
job_id='12345', first_seen='2025-06-20', last_seen='2025-06-22', payload={...}

-- Table 2: bronze.job_sightings (tracking appearances)
job_id='12345', seen_date='2025-06-20', is_new_posting=true
job_id='12345', seen_date='2025-06-21', is_new_posting=false
job_id='12345', seen_date='2025-06-22', is_new_posting=false
```

## **📊 Analysis of Each Approach**

### **Option 1: Keep All Records**

**✅ Pros:**

- Simple to implement (your current approach)
- Complete audit trail
- Can detect payload changes over time
- Easy to query "what did we see on day X"

**❌ Cons:**

- Storage bloat (lots of duplicate data)
- Complex queries (need DISTINCT everywhere)
- Hard to count "unique jobs per city"
- Duplicate key errors in silver schema

**Good for:** Detailed change tracking, compliance, debugging

### **Option 2: Single Record + Metadata**

**✅ Pros:**

- Efficient storage
- Easy to count unique jobs
- Simple silver schema integration
- Clear job lifecycle tracking

**❌ Cons:**

- Lose detailed daily snapshots
- Can't track payload changes over time
- Complex update logic during ingestion
- Harder to detect market trends by day

**Good for:** Current state analysis, job counting, efficiency

### **Option 3: Hybrid Approach**

**✅ Pros:**

- Best of both worlds
- Efficient for job counting
- Preserves daily market snapshots
- Clean separation of concerns
- Perfect for your use case

**❌ Cons:**

- More complex schema
- Two tables to maintain
- Slightly more complex queries

**Good for:** Market analysis, job seeker recommendations, trend analysis

## **🎯 Recommended Approach: Option 3 (Hybrid)**

For your "best city for job seekers" analysis, I recommend the hybrid approach:

### **New Bronze Schema Design:**

```sql
-- Table 1: Unique job postings
CREATE TABLE bronze.job_postings (
    source TEXT NOT NULL,
    job_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    days_active INTEGER GENERATED ALWAYS AS (
        EXTRACT(days FROM (last_seen - first_seen)) + 1
    ) STORED,
    is_active BOOLEAN DEFAULT true,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (source, job_id)
);

-- Table 2: Daily job market snapshots
CREATE TABLE bronze.job_sightings (
    source TEXT NOT NULL,
    job_id TEXT NOT NULL,
    seen_date DATE NOT NULL,
    is_new_posting BOOLEAN NOT NULL,
    api_call_timestamp TIMESTAMP NOT NULL,

    PRIMARY KEY (source, job_id, seen_date),
    FOREIGN KEY (source, job_id) REFERENCES bronze.job_postings(source, job_id)
);

-- Table 3: Daily market metrics (derived)
CREATE TABLE bronze.daily_market_metrics (
    source TEXT NOT NULL,
    date DATE NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    total_jobs INTEGER NOT NULL,
    new_jobs INTEGER NOT NULL,
    active_jobs INTEGER NOT NULL,

    PRIMARY KEY (source, date, city, state)
);
```

### **Updated Ingestion Logic:**

```python
def ingest_jobs_hybrid_approach(jobs_data, source, fetch_timestamp):
    """
    Ingest jobs using hybrid approach:
    1. Upsert to job_postings (one record per unique job)
    2. Insert to job_sightings (daily tracking)
    3. Update daily_market_metrics
    """

    today = fetch_timestamp.date()

    for job in jobs_data:
        job_id = job['id']
        city = extract_city(job)
        state = extract_state(job)

        # 1. Upsert to job_postings
        existing_job = get_existing_job(source, job_id)

        if existing_job:
            # Job exists - update last_seen
            update_job_posting(source, job_id, fetch_timestamp, job)
            is_new_posting = False
        else:
            # New job - insert
            insert_job_posting(source, job_id, fetch_timestamp, job)
            is_new_posting = True

        # 2. Insert to job_sightings
        insert_job_sighting(source, job_id, today, is_new_posting, fetch_timestamp)

        # 3. Update daily metrics (can be done in batch later)
        update_daily_metrics(source, today, city, state, is_new_posting)

def update_job_posting(source, job_id, fetch_timestamp, job_data):
    """Update existing job with new last_seen date"""
    query = """
    UPDATE bronze.job_postings
    SET
        last_seen = %s,
        updated_at = %s,
        payload = %s  -- Update payload in case job details changed
    WHERE source = %s AND job_id = %s
    """
    execute_query(query, (fetch_timestamp, fetch_timestamp, json.dumps(job_data), source, job_id))

def insert_job_posting(source, job_id, fetch_timestamp, job_data):
    """Insert new job posting"""
    query = """
    INSERT INTO bronze.job_postings (source, job_id, payload, first_seen, last_seen)
    VALUES (%s, %s, %s, %s, %s)
    """
    execute_query(query, (source, job_id, json.dumps(job_data), fetch_timestamp, fetch_timestamp))
```

## **📈 Benefits for Your Analysis**

### **1. Easy Job Counting by City:**

```sql
-- How many unique jobs in each city?
SELECT
    city,
    state,
    COUNT(*) as total_unique_jobs
FROM bronze.job_postings jp
JOIN bronze.job_sightings js ON jp.source = js.source AND jp.job_id = js.job_id
WHERE jp.is_active = true
GROUP BY city, state
ORDER BY total_unique_jobs DESC;
```

### **2. Market Trend Analysis:**

```sql
-- Job market trends over time
SELECT
    date,
    city,
    total_jobs,
    new_jobs,
    (new_jobs::float / total_jobs) as new_job_rate
FROM bronze.daily_market_metrics
WHERE city IN ('Chicago', 'Detroit')
ORDER BY date;
```

### **3. Job Persistence Analysis:**

```sql
-- Which cities have more stable job postings?
SELECT
    city,
    state,
    AVG(days_active) as avg_job_duration,
    COUNT(CASE WHEN days_active >= 7 THEN 1 END) as jobs_lasting_week_plus
FROM bronze.job_postings jp
WHERE is_active = true
GROUP BY city, state
ORDER BY avg_job_duration DESC;
```

### **4. Market Velocity:**

```sql
-- Which cities have the most active job markets?
SELECT
    city,
    state,
    AVG(new_jobs) as avg_daily_new_jobs,
    AVG(total_jobs) as avg_total_jobs
FROM bronze.daily_market_metrics
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY city, state
ORDER BY avg_daily_new_jobs DESC;
```

## **🔄 Migration Strategy**

### **Phase 1: Implement New Schema**

```sql
-- Create new tables alongside existing ones
-- Don't drop bronze.adzuna_jobs yet
```

### **Phase 2: Dual Write**

```python
# Update your ingest functions to write to both old and new schemas
# This gives you time to test and validate
```

### **Phase 3: Update Enrichment**

```python
# Update enrichment to read from bronze.job_postings instead of bronze.adzuna_jobs
def run_adzuna_enrichment():
    query = """
    SELECT jp.job_id, jp.payload
    FROM bronze.job_postings jp
    LEFT JOIN silver.jobs sj ON sj.source = jp.source AND sj.job_id = jp.job_id
    WHERE jp.source = 'adzuna' AND sj.job_id IS NULL
    """
    # No more duplicate issues!
```

### **Phase 4: Drop Old Schema**

```sql
-- Once everything works, drop the old table
DROP TABLE bronze.adzuna_jobs;
```

This hybrid approach gives you the best foundation for your "best city for job seekers" analysis while solving your duplicate key problem!

```sql
-- Same job_id appears multiple times with different fetched_at
job_id='12345', fetched_at='2025-06-20', payload={...}
job_id='12345', fetched_at='2025-06-21', payload={...}
job_id='12345', fetched_at='2025-06-22', payload={...}
```

```sql
-- One record per job_id, update metadata when seen again
job_id='12345', first_seen='2025-06-20', last_seen='2025-06-22', days_active=3, payload={...}
```

```sql
-- Keep one "master" record + track sightings separately
-- Table 1: bronze.job_postings (unique jobs)
job_id='12345', first_seen='2025-06-20', last_seen='2025-06-22', payload={...}

-- Table 2: bronze.job_sightings (tracking appearances)
job_id='12345', seen_date='2025-06-20', is_new_posting=true
job_id='12345', seen_date='2025-06-21', is_new_posting=false
job_id='12345', seen_date='2025-06-22', is_new_posting=false
```

```sql
-- Table 1: Unique job postings
CREATE TABLE bronze.job_postings (
    source TEXT NOT NULL,
    job_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    days_active INTEGER GENERATED ALWAYS AS (
        EXTRACT(days FROM (last_seen - first_seen)) + 1
    ) STORED,
    is_active BOOLEAN DEFAULT true,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (source, job_id)
);

-- Table 2: Daily job market snapshots
CREATE TABLE bronze.job_sightings (
    source TEXT NOT NULL,
    job_id TEXT NOT NULL,
    seen_date DATE NOT NULL,
    is_new_posting BOOLEAN NOT NULL,
    api_call_timestamp TIMESTAMP NOT NULL,

    PRIMARY KEY (source, job_id, seen_date),
    FOREIGN KEY (source, job_id) REFERENCES bronze.job_postings(source, job_id)
);

-- Table 3: Daily market metrics (derived)
CREATE TABLE bronze.daily_market_metrics (
    source TEXT NOT NULL,
    date DATE NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    total_jobs INTEGER NOT NULL,
    new_jobs INTEGER NOT NULL,
    active_jobs INTEGER NOT NULL,

    PRIMARY KEY (source, date, city, state)
);
```

```python
def ingest_jobs_hybrid_approach(jobs_data, source, fetch_timestamp):
    """
    Ingest jobs using hybrid approach:
    1. Upsert to job_postings (one record per unique job)
    2. Insert to job_sightings (daily tracking)
    3. Update daily_market_metrics
    """

    today = fetch_timestamp.date()

    for job in jobs_data:
        job_id = job['id']
        city = extract_city(job)
        state = extract_state(job)

        # 1. Upsert to job_postings
        existing_job = get_existing_job(source, job_id)

        if existing_job:
            # Job exists - update last_seen
            update_job_posting(source, job_id, fetch_timestamp, job)
            is_new_posting = False
        else:
            # New job - insert
            insert_job_posting(source, job_id, fetch_timestamp, job)
            is_new_posting = True

        # 2. Insert to job_sightings
        insert_job_sighting(source, job_id, today, is_new_posting, fetch_timestamp)

        # 3. Update daily metrics (can be done in batch later)
        update_daily_metrics(source, today, city, state, is_new_posting)

def update_job_posting(source, job_id, fetch_timestamp, job_data):
    """Update existing job with new last_seen date"""
    query = """
    UPDATE bronze.job_postings
    SET
        last_seen = %s,
        updated_at = %s,
        payload = %s  -- Update payload in case job details changed
    WHERE source = %s AND job_id = %s
    """
    execute_query(query, (fetch_timestamp, fetch_timestamp, json.dumps(job_data), source, job_id))

def insert_job_posting(source, job_id, fetch_timestamp, job_data):
    """Insert new job posting"""
    query = """
    INSERT INTO bronze.job_postings (source, job_id, payload, first_seen, last_seen)
    VALUES (%s, %s, %s, %s, %s)
    """
    execute_query(query, (source, job_id, json.dumps(job_data), fetch_timestamp, fetch_timestamp))
```

```sql
-- How many unique jobs in each city?
SELECT
    city,
    state,
    COUNT(*) as total_unique_jobs
FROM bronze.job_postings jp
JOIN bronze.job_sightings js ON jp.source = js.source AND jp.job_id = js.job_id
WHERE jp.is_active = true
GROUP BY city, state
ORDER BY total_unique_jobs DESC;
```

```sql
-- Job market trends over time
SELECT
    date,
    city,
    total_jobs,
    new_jobs,
    (new_jobs::float / total_jobs) as new_job_rate
FROM bronze.daily_market_metrics
WHERE city IN ('Chicago', 'Detroit')
ORDER BY date;
```

```sql
-- Which cities have more stable job postings?
SELECT
    city,
    state,
    AVG(days_active) as avg_job_duration,
    COUNT(CASE WHEN days_active >= 7 THEN 1 END) as jobs_lasting_week_plus
FROM bronze.job_postings jp
WHERE is_active = true
GROUP BY city, state
ORDER BY avg_job_duration DESC;
```

```sql
-- Which cities have the most active job markets?
SELECT
    city,
    state,
    AVG(new_jobs) as avg_daily_new_jobs,
    AVG(total_jobs) as avg_total_jobs
FROM bronze.daily_market_metrics
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY city, state
ORDER BY avg_daily_new_jobs DESC;
```

```sql
-- Create new tables alongside existing ones
-- Don't drop bronze.adzuna_jobs yet
```

```python
# Update your ingest functions to write to both old and new schemas
# This gives you time to test and validate
```

```python
# Update enrichment to read from bronze.job_postings instead of bronze.adzuna_jobs
def run_adzuna_enrichment():
    query = """
    SELECT jp.job_id, jp.payload
    FROM bronze.job_postings jp
    LEFT JOIN silver.jobs sj ON sj.source = jp.source AND sj.job_id = jp.job_id
    WHERE jp.source = 'adzuna' AND sj.job_id IS NULL
    """
    # No more duplicate issues!
```

```sql
-- Once everything works, drop the old table
DROP TABLE bronze.adzuna_jobs;
```

---

---

---

**User**

please walk me through the entire project now and explain how everything works. make sure to explain each file needed now and what files are no longer needed

---

**Cursor**

I'll walk you through the entire project structure and explain how everything works after the v2 migration. Let me start by examining the current state and then provide a comprehensive overview.

Now let me provide you with a comprehensive walkthrough of the entire project:

# GFCI Prototype Project - Complete Overview

## 🏗️ Project Architecture

Your project follows a **medallion architecture** with Bronze → Silver → Gold data layers:

- **Bronze**: Raw data from APIs and CSV files
- **Silver**: Cleaned, enriched, and standardized data
- **Gold**: Analysis-ready aggregated data for dashboards and reporting

## 📁 Core Project Structure

### **Main Entry Point**

- **`main.py`** ✅ **ACTIVE** - Central command-line interface for the entire pipeline

### **Data Ingestion Layer** (`ingest/`)

- **`ingest_adzuna_v2.py`** ✅ **ACTIVE** - V2 Adzuna job ingestion (current)
- **`ingest_housing_data.py`** ✅ **ACTIVE** - Zillow housing data ingestion
- **`ingest_jsearch.py`** ✅ **ACTIVE** - JSearch job API ingestion
- **`ingest_adzuna.py`** ⚠️ **LEGACY** - Old Adzuna ingestion (can be removed)

### **Data Transformation Layer** (`transform/`)

- **`enrich_adzuna_v2.py`** ✅ **ACTIVE** - V2 Adzuna enrichment (current)
- **`enrich_housing_data.py`** ✅ **ACTIVE** - Housing data enrichment
- **`enrich_jsearch.py`** ✅ **ACTIVE** - JSearch enrichment
- **`utils.py`** ✅ **ACTIVE** - Shared enrichment utilities
- **`enrich_adzuna.py`** ⚠️ **LEGACY** - Old Adzuna enrichment (can be removed)

### **Database Layer** (`database/`)

- **`db.py`** ✅ **ACTIVE** - Database connection and utilities

### **SQL Schema** (`sql/`)

- **`0_init_schema.sql`** ✅ **ACTIVE** - Creates schemas (bronze, silver, gold)
- **`2_create_tables_v2.sql`** ✅ **ACTIVE** - V2 table definitions (current)
- **`clear_silver_tables.sql`** ✅ **ACTIVE** - Development utility
- **`1_create_tables.sql`** ⚠️ **LEGACY** - Old table definitions
- **`queries/v2_sample_queries.sql`** ✅ **ACTIVE** - Example analytics queries

### **Migration & Scripts** (`scripts/`)

- **`migrate_to_v2.py`** ✅ **ACTIVE** - Migration script (used once)

### **Data Files** (`data/`)

- Various Zillow CSV files for housing market data

---

## 🔄 How the Complete Pipeline Works

### **1. Database Setup (Run Once)**

```bash
# Initialize database schemas
python main.py --init

# Create all tables
python main.py --create
```

**What happens:**

- Creates `bronze`, `silver`, `gold` schemas
- Creates all v2 tables with proper indexes and triggers

### **2. Data Ingestion**

```bash
# Ingest job data (makes API calls)
python main.py --ingest-jobs

# Ingest housing data (from CSV files)
python main.py --ingest-housing
```

**Job Ingestion Flow (`ingest_adzuna_v2.py`):**

1. Calls Adzuna API for each city/role combination
2. Records each API call in `bronze.job_calls` (audit trail)
3. Upserts jobs into `bronze.raw_jobs` (no duplicates)
4. Tracks job lifecycle: `first_seen`, `last_seen`, `times_seen`

**Housing Ingestion Flow (`ingest_housing_data.py`):**

1. Reads Zillow CSV files (ZORI rent data, ZHVI home values)
2. Stores raw data in `bronze.zillow_*` tables

### **3. Data Enrichment**

```bash
# Enrich job data
python main.py --enrich-jobs

# Enrich housing data
python main.py --enrich-housing
```

**Job Enrichment Flow (`enrich_adzuna_v2.py`):**

1. Reads raw JSON from `bronze.raw_jobs`
2. Parses and cleans all fields
3. Applies business logic using `utils.py`:
   - Determines seniority level (jr/mid/sr)
   - Detects remote work
   - Categorizes industry
   - Extracts years of experience
   - Parses education requirements
4. Upserts into `silver.jobs_v2`
5. **Automatically updates gold aggregations:**
   - `gold.city_job_stats` - Daily city-level metrics
   - `gold.latest_city_snapshot` - Current market state

**Housing Enrichment Flow (`enrich_housing_data.py`):**

1. Processes raw housing data
2. Standardizes city names and codes
3. Stores in `silver.housing_metrics`

---

## 🗄️ Database Schema (V2)

### **Bronze Schema (Raw Data)**

```sql
bronze.job_calls        -- API call audit trail
bronze.raw_jobs         -- Unique jobs with lifecycle tracking
bronze.zillow_zori      -- Raw Zillow rent data
bronze.zillow_zhvi      -- Raw Zillow home value data
```

### **Silver Schema (Cleaned Data)**

```sql
silver.jobs_v2          -- Parsed & enriched job data
silver.housing_metrics  -- Cleaned housing data
```

### **Gold Schema (Analytics Ready)**

```sql
gold.city_job_stats        -- Daily city-level job aggregations
gold.latest_city_snapshot  -- Current market snapshots
```

---

## 🎯 Key Features of V2 Architecture

### **1. No Duplicate Jobs**

- `bronze.raw_jobs` uses upsert logic
- Each job has a lifecycle: first_seen → last_seen → times_seen
- Jobs are marked active/inactive based on latest API calls

### **2. Market Intelligence**

- `bronze.job_calls` tracks API metadata (total job count, mean salary)
- Provides market size signals beyond just the jobs we store

### **3. Performance Optimized**

- Gold tables are pre-aggregated for fast dashboard queries
- Proper indexing on common query patterns

### **4. Analysis Ready**

The gold schema provides instant answers to questions like:

- Which cities have the most jobs?
- What's the salary range in each market?
- How is the job market growing?
- What's the remote work percentage?

---

## 📊 Sample Analytics Queries

The project includes ready-to-use queries in `sql/queries/v2_sample_queries.sql`:

```sql
-- Top cities by active jobs
SELECT city, state, active_jobs, avg_salary
FROM gold.latest_city_snapshot
ORDER BY active_jobs DESC;

-- Job market trends
SELECT run_date, active_jobs, new_jobs
FROM gold.city_job_stats
WHERE city = 'Chicago'
ORDER BY run_date DESC;

-- Remote work analysis
SELECT city,
       ROUND(100.0 * remote_jobs / active_jobs, 2) as remote_percentage
FROM gold.city_job_stats
ORDER BY remote_percentage DESC;
```

---

## 🗑️ Files That Can Be Removed

### **Legacy Files (No Longer Needed):**

- `ingest/ingest_adzuna.py` - Replaced by `ingest_adzuna_v2.py`
- `transform/enrich_adzuna.py` - Replaced by `enrich_adzuna_v2.py`
- `sql/1_create_tables.sql` - Replaced by `sql/2_create_tables_v2.sql`
- `prototype.py`, `prototype copy.py`, `prototype2.py` - Old development files

### **Development Files:**

- `test.py` - Can be removed if not needed
- Various empty SQL files in `sql/` directory

---

## 🚀 Daily Operations

### **Typical Daily Workflow:**

```bash
# 1. Ingest fresh job data
python main.py --ingest-jobs

# 2. Process into analytics tables
python main.py --enrich-jobs

# 3. Ingest housing data (as needed)
python main.py --ingest-housing
python main.py --enrich-housing
```

### **Development Workflow:**

```bash
# Clear processed data for testing
python main.py --clear-silver

# Re-run enrichment
python main.py --enrich-jobs
```

---

## 💡 Key Benefits of Current Architecture

1. **Scalable**: Easy to add new job sources or cities
2. **Reliable**: Proper error handling and data validation
3. **Efficient**: No duplicate processing, smart upserts
4. **Insightful**: Rich analytics via gold schema
5. **Maintainable**: Clean separation of concerns
6. **Auditable**: Complete API call history

The project is now fully migrated to v2 and ready for production use! The architecture supports both real-time job market analysis and historical trend tracking.

```shellscript
# Initialize database schemas
python main.py --init

# Create all tables
python main.py --create
```

```shellscript
# Ingest job data (makes API calls)
python main.py --ingest-jobs

# Ingest housing data (from CSV files)
python main.py --ingest-housing
```

```shellscript
# Enrich job data
python main.py --enrich-jobs

# Enrich housing data
python main.py --enrich-housing
```

```sql
bronze.job_calls        -- API call audit trail
bronze.raw_jobs         -- Unique jobs with lifecycle tracking
bronze.zillow_zori      -- Raw Zillow rent data
bronze.zillow_zhvi      -- Raw Zillow home value data
```

```sql
silver.jobs_v2          -- Parsed & enriched job data
silver.housing_metrics  -- Cleaned housing data
```

```sql
gold.city_job_stats        -- Daily city-level job aggregations
gold.latest_city_snapshot  -- Current market snapshots
```

```sql
-- Top cities by active jobs
SELECT city, state, active_jobs, avg_salary
FROM gold.latest_city_snapshot
ORDER BY active_jobs DESC;

-- Job market trends
SELECT run_date, active_jobs, new_jobs
FROM gold.city_job_stats
WHERE city = 'Chicago'
ORDER BY run_date DESC;

-- Remote work analysis
SELECT city,
       ROUND(100.0 * remote_jobs / active_jobs, 2) as remote_percentage
FROM gold.city_job_stats
ORDER BY remote_percentage DESC;
```

```shellscript
# 1. Ingest fresh job data
python main.py --ingest-jobs

# 2. Process into analytics tables
python main.py --enrich-jobs

# 3. Ingest housing data (as needed)
python main.py --ingest-housing
python main.py --enrich-housing
```

```shellscript
# Clear processed data for testing
python main.py --clear-silver

# Re-run enrichment
python main.py --enrich-jobs
```
