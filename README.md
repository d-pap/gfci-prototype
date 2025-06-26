# Job Market Intelligence Pipeline

A data pipeline that helps data professionals find the best cities to work in by analyzing job availability, salary trends, and cost of living. Built to answer: **_"Where should I move for the best career opportunities I can actually afford?"_**

## What This Does

This system automatically collects job postings from multiple sources, tracks how salaries change over time, and compares them against local housing costs. I built this because most job market analysis looks at either salaries OR cost of living, but rarely both together in a way that's useful for making actual decisions.

The pipeline runs daily, processing hundreds of job postings and maintaining a historical view of how different markets evolve. It can spot when a city's job market is heating up, when salaries are trending upward, or when the competition for roles is getting tighter.

The aim is to provide insights into things like:

- Job availability by city
- Salary ranges for new graduates
- Cost of living vs. salary ratios
- Industry trends and company insights
- And more

## Data Sources

- Adzuna API: Job postings data
- JSearch API (RapidAPI): Additional job postings
- Zillow CSV: Housing costs and rental prices
- BLS Data: Economic indicators (planned)

## How It Works

**Data Collection**: Pulls job postings from Adzuna API and housing data from Zillow research files. Each job gets tracked over time - when it first appeared, how long it stayed active, and whether it's still available.

**Smart Processing**: Jobs get automatically classified by seniority level, remote work options, and industry. The system handles duplicates intelligently and tracks the lifecycle of each posting.

**Market Analysis**: Aggregates everything into city-level insights including job growth rates, salary ranges, remote work percentages, and affordability ratios.

## Project Structure

```
gfci/
├── main.py                     # Command-line interface for all operations
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables
├── README.md                   # This file
│
├── ingest/                     # Data collection from APIs and files
│   ├── ingest_adzuna_v2.py     # Adzuna job API integration
│   ├── ingest_housing_data.py  # Zillow housing data loader
│   └── ingest_jsearch.py       # JSearch API (secondary source)
│
├── transform/                  # Data cleaning and enrichment
│   ├── enrich_adzuna_v2.py     # Job data processing and analysis
│   ├── enrich_housing_data.py  # Housing data standardization
│   └── utils.py                # Shared logic for job classification
│
├── database/                   # Database connection utilities
│   └── db.py
│
├── sql/                        # Database schema and queries
│   ├── 0_init_schema.sql       # Creates database schemas
│   ├── 2_create_tables_v2.sql  # Table definitions
│   └── queries/                # Analysis queries
│
├── data/                       # CSV files
│   └── housing/                # Zillow CSV files
│
├── notebooks/                  # Jupyter notebooks for analysis
│   └── analysis.ipynb          # Main analysis notebook
│
└── docs/                       # Documentation files
```

## Setup

**Requirements**: Python 3.8+, PostgreSQL

```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your database credentials and API keys

# Initialize database
python main.py --init
python main.py --create
```

**API Keys Needed**:

- Adzuna API (free tier available)
- Optional: RapidAPI key for JSearch

## Usage

```bash
# Collect fresh job data
python main.py --ingest-jobs

# Process and analyze the data
python main.py --enrich-jobs

# Load housing market data
python main.py --ingest-housing
python main.py --enrich-housing
```

The system currently tracks Detroit and Chicago markets for data analyst, data scientist, and business analyst roles. Adding new cities or job types is straightforward.

## Database Design

**Bronze Layer**: Raw data exactly as received from APIs, with full audit trails of when each job was seen.

**Silver Layer**: Cleaned and standardized data with derived fields like seniority level, remote work classification, and industry categorization.

**Gold Layer**: Pre-aggregated analytics tables for fast querying - city-level job counts, salary statistics, market trends, and growth rates.

## Key Features

**Job Lifecycle Tracking**: Knows when each job first appeared, how long it stayed active, and can calculate market velocity.

**Duplicate Handling**: Same job posted multiple times gets tracked as one posting with updated timestamps.

**Market Intelligence**: Captures not just the jobs you can see, but metadata about total market size from API responses.

**Salary Analysis**: Tracks salary ranges, calculates medians and averages, and identifies trends over time.

**Cost of Living Integration**: Combines job data with housing costs to calculate actual affordability.

## Analysis Examples

```sql
-- Cities with the most active job markets
SELECT city, state, active_jobs, avg_salary
FROM gold.latest_city_snapshot
ORDER BY active_jobs DESC;

-- Market growth trends
SELECT run_date, city, new_jobs, job_growth_rate_7d
FROM gold.city_job_stats
WHERE city = 'Chicago'
ORDER BY run_date DESC;

-- Remote work opportunities by city
SELECT city,
       ROUND(100.0 * remote_jobs / active_jobs, 2) as remote_percentage
FROM gold.city_job_stats
WHERE run_date = CURRENT_DATE
ORDER BY remote_percentage DESC;
```

## Technical Details

Built with Python and PostgreSQL. Uses a medallion architecture (bronze/silver/gold) for data quality and performance. All operations are idempotent - safe to re-run if something fails.

The enrichment logic handles real-world messiness: inconsistent job titles, varying salary formats, different location naming conventions, and duplicate postings across time periods.

Rate limiting and error handling ensure the system can run reliably as a daily job without hitting API limits or failing on individual bad records.

## Future Plans

- [ ] Expand to more cities and job categories
- [ ] Add BLS economic data integration
- [ ] Add automated scheduling with Airflow/Cron
- [ ] Create web dashboard for real-time insights
- [ ] Add machine learning for salary prediction
- [ ] Integrate additional data sources for market context

## Contributing

The modular design makes it easy to add new data sources or analysis dimensions. Each component has a clear responsibility and the database schema supports extensibility.
