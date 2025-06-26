import time
from datetime import datetime, timezone, date
import pandas as pd
import requests
from database.db import get_engine
import os
import json
from sqlalchemy import MetaData, Table
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
    return r.json().get("results", [])

def iter_jobs(city, role, max_pages=20):
    """Iterate through all pages of jobs for a given (city, role) pair"""
    for p in range(1, max_pages + 1):
        rows = fetch_page(city, role, p)
        if not rows:
            break
        yield from rows
        time.sleep(0.5)
        
def normalize_job(job):
    """Normalize job data for insertion"""
    return {
        'source': 'adzuna',
        'job_id': job['id'],
        'fetched_at': date.today(),
        'payload': json.dumps(job)
    }
    
def insert_rows(rows):
    """
    Insert rows into the adzuna_jobs table
    """
    if not rows:
        return
    meta = MetaData()
    table = Table('adzuna_jobs', meta, autoload_with=get_engine(), schema='bronze')
    stmt = pg_insert(table).values(rows).on_conflict_do_nothing(index_elements=['job_id', 'fetched_at'])
    with get_engine().begin() as conn:
        conn.execute(stmt)

def ingest_adzuna(city, roles, max_pages=20):
    """Ingest one (city, role) pair"""
    jobs = [normalize_job(j) for j in iter_jobs(city, roles, max_pages)]
    insert_rows(jobs)
    print(f"Ingested {len(jobs)} jobs for {city} and roles {roles}")