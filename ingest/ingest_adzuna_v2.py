"""
Adzuna API integration v2
Implements the new bronze schema design with job_calls and raw_jobs tables

Stores a record of the API call once per city/role/day
Only records API calls if there are new jobs found
"""
import time
from datetime import date
import requests
from database.db import get_engine
import os
import json
from sqlalchemy import text

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

def get_existing_job_ids_for_today(source, run_date):
    """Get all job IDs we've already seen today"""
    engine = get_engine()
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT job_id 
            FROM bronze.raw_jobs 
            WHERE source = :source 
            AND last_seen = :run_date
        """), {
            'source': source,
            'run_date': run_date
        })
        
        return {row.job_id for row in result}

def filter_new_jobs(jobs, source, run_date):
    """Filter out jobs we've already processed today"""
    existing_today = get_existing_job_ids_for_today(source, run_date)
    
    new_jobs = []
    for job in jobs:
        job_id = str(job['id'])
        if job_id not in existing_today:
            new_jobs.append(job)
    
    return new_jobs

def record_api_call_if_new_jobs(city, role, pages_fetched, api_response, run_date, jobs_retrieved, new_jobs_count):
    """Only record API call if there were new jobs found"""
    
    if new_jobs_count == 0:
        print(f"  No new jobs found for {role} in {city} - skipping job_calls update")
        return
    
    engine = get_engine()
    api_count = api_response.get('count', 0)
    api_mean_salary = api_response.get('mean', None)
    
    with engine.begin() as conn:
        # Upsert: one row per city/role/day
        conn.execute(text("""
            INSERT INTO bronze.job_calls (run_date, city, role, pages_fetched, api_count, api_mean_salary, jobs_retrieved)
            VALUES (:run_date, :city, :role, :pages_fetched, :api_count, :api_mean_salary, :jobs_retrieved)
            ON CONFLICT (run_date, city, role) DO UPDATE SET
                pages_fetched = EXCLUDED.pages_fetched,
                api_count = EXCLUDED.api_count,
                api_mean_salary = EXCLUDED.api_mean_salary,
                jobs_retrieved = bronze.job_calls.jobs_retrieved + EXCLUDED.jobs_retrieved,
                pulled_at = CURRENT_TIMESTAMP
        """), {
            'run_date': run_date,
            'city': city,
            'role': role,
            'pages_fetched': pages_fetched,
            'api_count': api_count,
            'api_mean_salary': api_mean_salary,
            'jobs_retrieved': new_jobs_count  # Only count NEW jobs
        })

def upsert_raw_jobs_v2_optimized(jobs, run_date):
    """Insert or update jobs, but only process new jobs for today"""
    if not jobs:
        return 0
        
    engine = get_engine()
    new_jobs_processed = 0
    
    # Pre-filter: only process jobs we haven't seen today
    new_jobs = filter_new_jobs(jobs, 'adzuna', run_date)
    
    if not new_jobs:
        return 0  # No new jobs to process
    
    with engine.begin() as conn:
        for job in new_jobs:
            job_id = str(job['id'])
            
            # Check if job exists at all (any day)
            result = conn.execute(text("""
                SELECT first_seen, last_seen, times_seen 
                FROM bronze.raw_jobs 
                WHERE source = 'adzuna' AND job_id = :job_id
            """), {'job_id': job_id})
            
            existing_job = result.fetchone()
            
            if existing_job:
                # Job exists from previous day - update last_seen and increment times_seen
                last_seen_date = existing_job.last_seen
                
                if last_seen_date < run_date:
                    # New day for this job
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
                    new_jobs_processed += 1
                # If last_seen == run_date, we already processed this job today (shouldn't happen due to pre-filter)
                
            else:
                # Completely new job - insert
                conn.execute(text("""
                    INSERT INTO bronze.raw_jobs (job_id, source, first_seen, last_seen, times_seen, payload)
                    VALUES (:job_id, 'adzuna', :first_seen, :last_seen, 1, :payload)
                """), {
                    'job_id': job_id,
                    'first_seen': run_date,
                    'last_seen': run_date,
                    'payload': json.dumps(job)
                })
                new_jobs_processed += 1
    
    return new_jobs_processed

def upsert_raw_jobs_v2(jobs, run_date):
    """Insert or update jobs, only increment times_seen for new days"""
    engine = get_engine()
    
    with engine.begin() as conn:
        for job in jobs:
            job_id = str(job['id'])
            
            result = conn.execute(text("""
                SELECT first_seen, last_seen, times_seen 
                FROM bronze.raw_jobs 
                WHERE source = 'adzuna' AND job_id = :job_id
            """), {'job_id': job_id})
            
            existing_job = result.fetchone()
            
            if existing_job:
                last_seen_date = existing_job.last_seen
                
                # Only increment times_seen if this is a new day
                if last_seen_date < run_date:
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
                    # Same day - just update payload, don't increment times_seen
                    conn.execute(text("""
                        UPDATE bronze.raw_jobs 
                        SET payload = :payload,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE source = 'adzuna' AND job_id = :job_id
                    """), {
                        'job_id': job_id,
                        'payload': json.dumps(job)
                    })
            else:
                # New job - insert normally
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
    Ingest Adzuna jobs with optimized duplicate handling
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
            
            # Capture metadata from first successful response
            if api_metadata is None:
                api_metadata = api_response
            
            jobs = api_response.get('results', [])
            
            if not jobs:
                print(f"No more results after page {page - 1}")
                break
                
            all_jobs.extend(jobs)
            print(f"  Page {page}: Got {len(jobs)} jobs")
            time.sleep(0.5)  # Rate limiting
            pages_fetched += 1
            
        except Exception as e:
            print(f"Error on page {page}: {e}")
            break
    
    # Process only new jobs
    new_jobs_count = upsert_raw_jobs_v2_optimized(all_jobs, run_date)
    
    # Only record API call if we found new jobs
    if api_metadata and new_jobs_count > 0:
        record_api_call_if_new_jobs(city, role, pages_fetched, api_metadata, run_date, len(all_jobs), new_jobs_count)
        print(f"Total: Found {len(all_jobs)} jobs, {new_jobs_count} were new for {role} in {city}")
    elif new_jobs_count == 0:
        print(f"Total: Found {len(all_jobs)} jobs, but all were already processed today for {role} in {city}")
    
    return new_jobs_count  # Return count of NEW jobs, not total jobs

# Backward compatibility - keep old function name during migration
def ingest_adzuna(city, role, max_pages=20):
    """Wrapper for backward compatibility"""
    return ingest_adzuna_v2(city, role, max_pages)

if __name__ == "__main__":
    # Test the function
    ingest_adzuna_v2("Chicago, IL", "data analyst", max_pages=2) 