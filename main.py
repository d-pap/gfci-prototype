"""
look at projects/gfci-modular for better code structure

# TODO:
0. GET NUMBER OF TOTAL JOBS IN CITIES NOT JUST 50 PER PAGE
1. update tables to include new fields from zillow and jsearch (e.g. rent, housing_type, etc.)
2. add new data sources 
3. add new enrichment logic
4. add new utils in utils.py
"""
from ingest.ingest_adzuna_v2 import ingest_adzuna
from ingest.ingest_housing_data import ingest_zillow_csv
from transform.enrich_housing_data import enrich_zori, enrich_zhvi
from database.db import init_schema, create_tables, run_sql_file, get_engine, text
from pathlib import Path
from transform.enrich_adzuna_v2 import run_adzuna_enrichment_v2
from ingest.ingest_jsearch import ingest_jsearch
from transform.enrich_jsearch import run_jsearch_enrichment
import argparse

# Config
TARGET_CITIES = ["Detroit, MI", "Chicago, IL"]
TARGET_ROLES = [
        "data analyst", "data scientist", "business analyst"
        # "data analyst", "business analyst", "data scientist", 
        # "data engineer", "ml engineer", "quantitative analyst"
    ]
HOUSING_DATASETS = {
    'zori': Path('data/zori-6-19-2025.csv'),
    #'zhvi': Path('data/zhvi-6-19-2025.csv')
}

def ingest_all_jobs(cities, roles):
    """Ingest all jobs from all cities and roles and store in bronze schema"""
    for city in cities:
        for role in roles:
            # adzuna jobs 
            ingest_adzuna(city, role)
            
            # jsearch jobs
            #ingest_jsearch(city, role)

        
def enrich_all_jobs(cities, roles):
    """Enrich all jobs from all cities and roles and store in silver schema"""
    run_adzuna_enrichment_v2()
    #run_jsearch_enrichment()
    print(f'Enriched all jobs for cities: {cities} and roles: {roles}')
    
            
def ingest_all_housing(dataset_paths: dict):
    """ingest all housing csvs"""
    for dataset_name, csv_path in dataset_paths.items():
        table_name = f"zillow_{dataset_name}"
        ingest_zillow_csv(csv_path, table_name)
        
def enrich_all_housing():
    """enrich all housing data"""
    enrich_zori()
    #enrich_zhvi()
    # add other enrichment functions as needed here...


def clear_silver_tables():
    """Clear silver schema data for fresh processing during development"""
    
    #! using .sql file instead of raw sql method:
    #? does this work the same as the raw sql method???
    # run_sql_file(get_engine(), "sql/clear_silver_tables.sql")
    
    #! using raw sql method:
    engine = get_engine()
    with engine.begin() as con:
        con.execute(text("TRUNCATE TABLE silver.jobs"))
        con.execute(text("TRUNCATE TABLE silver.housing_metrics"))
        print("Cleared silver schema data")

def main():
    """
    Main function to run the project
    """
    # command line arguments
    parser = argparse.ArgumentParser(description="Data pipeline for job and housing data")
    
    # setup (run once)
    parser.add_argument("--init", action="store_true", help="Initialize database schema")
    parser.add_argument("--create", action="store_true", help="Create database tables")
    
    # fresh data ingestion
    parser.add_argument('--ingest-jobs', action='store_true', help='Ingest jobs (make API calls)')
    parser.add_argument('--ingest-housing', action='store_true', help='Ingest all housing data (no API call)')
    
    # process data
    parser.add_argument('--enrich-jobs', action='store_true', help='Enrich jobs (no API calls)')
    parser.add_argument('--enrich-housing', action='store_true', help='Enrich housing data (no API calls)')

    # full pipeline
    #parser.add_argument('--all', action='store_true', help='Run all ingest and enrich scripts')
    
    # DURING DEVELOPMENT    
    parser.add_argument('--clear-silver', action='store_true', help='Clear all tables in silver schema')
    
    
    args = parser.parse_args()
    
    if args.init:
        init_schema()
    elif args.create:
        create_tables()
    elif args.clear_silver:
        clear_silver_tables()
    elif args.ingest_jobs:
        ingest_all_jobs(TARGET_CITIES, TARGET_ROLES)
    elif args.enrich_jobs:
        enrich_all_jobs(TARGET_CITIES, TARGET_ROLES)
    elif args.ingest_housing:
        ingest_all_housing(HOUSING_DATASETS)
    elif args.enrich_housing:
        enrich_all_housing()
    else:
        parser.print_help()
    
    
    #! RUN ONCE UNLESS DB CHANGES
    # init_schema()
    # create_tables()
    
    #! ONCE PER DAY: commented out during development
    # Load raw jobs and process them
    #ingest_all_jobs(TARGET_CITIES, TARGET_ROLES)

    # # Enrich jobs
    # print("Enriching jobs...")
    # enrich_all_jobs(TARGET_CITIES, TARGET_ROLES)
    
    # Load zillow data
    # print("Loading zillow data...")
    # load_zori(Path('zori-6-19-2025.csv'))
    #fetch_zillow_zori(Path('data/zori-6-19-2025.csv'))
    
    # Commented out during development - use command line args instead
    # ingest_all_housing(HOUSING_DATASETS)
    # enrich_all_housing()
    

if __name__ == "__main__":
    main()
