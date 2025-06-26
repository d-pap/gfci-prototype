"""
Adzuna API integration v2 - writes to both old and new schemas during migration
Implements the new bronze schema design with job_calls and raw_jobs tables
"""
import time
from datetime import datetime, timezone, date
import pandas as pd
import requests
from database.db import get_engine
import os
import json
from sqlalchemy import MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

def fetch_page(city, role, page=1, per_page=50):
    """Fetch a single page of jobs from the Adzuna API"""
    url = f"https://api.adzuna.com/v1/api/jobs/us/search/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "what": role,
        "where": city,
        "results_per_page": per_page,
        "content-type": "application/json"
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def record_api_call(city, role, pages_fetched, api_response, run_date, jobs_retrieved):
    """Record the API call in bronze.job_calls table"""
    engine = get_engine()
    
    # Extract metadata from API response
    api_count = api_response.get('count', 0)
    api_mean_salary = api_response.get('mean', None)
    
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO bronze.job_calls (run_date, city, role, pages_fetched, api_count, api_mean_salary, jobs_retrieved)
            VALUES (:run_date, :city, :role, :pages_fetched, :api_count, :api_mean_salary, :jobs_retrieved)
        """), {
            'run_date': run_date,
            'city': city,
            'role': role,
            'pages_fetched': pages_fetched,
            'api_count': api_count,
            'api_mean_salary': api_mean_salary,
            'jobs_retrieved': jobs_retrieved
        })

def upsert_raw_jobs_v2(jobs, run_date):
    """Insert or update jobs in the new bronze.raw_jobs table"""
    if not jobs:
        return
        
    engine = get_engine()
    
    with engine.begin() as conn:
        for job in jobs:
            job_id = str(job['id'])
            
            # Check if job already exists
            result = conn.execute(text("""
                SELECT first_seen, last_seen, times_seen 
                FROM bronze.raw_jobs 
                WHERE source = 'adzuna' AND job_id = :job_id
            """), {'job_id': job_id})
            
            existing_job = result.fetchone()
            
            if existing_job:
                # Job exists - update last_seen and increment times_seen
                conn.execute(text("""
                    UPDATE bronze.raw_jobs 
                    SET last_seen = :last_seen,
                        times_seen = times_seen + 1,
                        payload = :payload,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE source = 'adzuna' AND job_id = :job_id
                """), {
                    'job_id': job_id,
                    'last_seen': run_date,
                    'payload': json.dumps(job)
                })
            else:
                # New job - insert
                conn.execute(text("""
                    INSERT INTO bronze.raw_jobs (job_id, source, first_seen, last_seen, times_seen, payload)
                    VALUES (:job_id, 'adzuna', :first_seen, :last_seen, 1, :payload)
                """), {
                    'job_id': job_id,
                    'first_seen': run_date,
                    'last_seen': run_date,
                    'payload': json.dumps(job)
                })

def ingest_adzuna_v2(city, role, max_pages=20):
    """
    Ingest Adzuna jobs - writes to both old and new schemas
    
    Args:
        city: City name (e.g., "Chicago, IL")
        role: Job role/title to search for
        max_pages: Maximum pages to fetch
    """
    run_date = date.today()
    all_jobs = []
    pages_fetched = 0
    api_metadata = None
    print(f"Fetching {role} jobs in {city}...")
    
    for page in range(1, max_pages + 1):
        try:
            # Fetch page from API
            api_response = fetch_page(city, role, page)
            
            # capture metadata from first successful response
            if api_metadata is None:
                api_metadata = api_response
            
            jobs = api_response.get('results', [])
            
            # Record the API call
            # record_api_call(city, role, page, api_response, run_date)
            
            if not jobs:
                print(f"No more results after page {page - 1}")
                break
                
            all_jobs.extend(jobs)
            
            # Insert into new schema (bronze.raw_jobs)
            upsert_raw_jobs_v2(jobs, run_date)

            
            print(f"  Page {page}: Got {len(jobs)} jobs")
            time.sleep(0.5)  # Rate limiting
            pages_fetched += 1
            
        except Exception as e:
            print(f"Error on page {page}: {e}")
            break
    
    # record the api call once at the end
    if api_metadata:
        record_api_call(city, role, pages_fetched, api_metadata, run_date, len(all_jobs))
    
    print(f"Total: Ingested {len(all_jobs)} jobs for {role} in {city}")
    return len(all_jobs)

# Backward compatibility - keep old function name during migration
def ingest_adzuna(city, role, max_pages=20):
    """Wrapper for backward compatibility"""
    return ingest_adzuna_v2(city, role, max_pages)

if __name__ == "__main__":
    # Test the function
    ingest_adzuna_v2("Chicago, IL", "data analyst", max_pages=2) 