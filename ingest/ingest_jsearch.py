"""
JSearch API job scraper (via RapidAPI)
"""
import requests
import os
from datetime import datetime, timezone, date
import pandas as pd
from database.db import get_engine
import json
import time
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

# url = "https://jsearch.p.rapidapi.com/search"

# querystring = {"query":"developer jobs in chicago","page":"1","num_pages":"1","country":"us","date_posted":"all"}

# headers = {
# 	"x-rapidapi-key": os.getenv('JSEARCH_API_KEY'),
# 	"x-rapidapi-host": "jsearch.p.rapidapi.com"
# }

# response = requests.get(url, headers=headers, params=querystring)

# print(response.json())

# def get_jobs_jsearch(role, location, page=1):
#     url = "https://jsearch.p.rapidapi.com/search"
#     headers = {
# 	"x-rapidapi-key": os.getenv('JSEARCH_API_KEY'),
# 	"x-rapidapi-host": "jsearch.p.rapidapi.com"
#     }
#     params = {
#         'query': f'{role} jobs in {location}',
#         'page': page,
#         'num_pages': 1,
#         'country': 'us',
#         'date_posted': 'all'
#     }
#     response = requests.get(url, headers=headers, params=params)
#     if response.status_code != 200:
#         print(f'Failed request: {response.status_code}')
#         return []
#     return response.json().get('data', [])

# def load_raw_jobs_jsearch(city, roles):
#     engine = get_engine()
#     raw_records = []
    
#     for role in roles:
#         jobs = get_jobs_jsearch(role, city)
#         for job in jobs:
#             raw_records.append({
#                 'source': 'jsearch',
#                 'external_id': job.get('job_id', ''),
#                 'payload': json.dumps(job),
#                 'inserted_at': datetime.now(timezone.utc)
#             })
#     if raw_records: 
#         pd.DataFrame(raw_records).to_sql('jobs_raw', engine, if_exists='append', index=False)
#         print(f'Loaded {len(raw_records)} raw jobs from JSearch')


def fetch_jsearch_page(city, role, page=1):
    url = "https://jsearch.p.rapidapi.com/search"
    headers ={
        'x-rapidapi-key': os.getenv('JSEARCH_API_KEY'),
        'x-rapidapi-host': 'jsearch.p.rapidapi.com'
    }
    params ={
        'query': f'{role} jobs in {city}',
        'page': page,
        'num_pages': 1,
        'country': 'us',
        'date_posted': 'all'
    }
    
    # make http request
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get('data', [])
    except requests.exceptions.RequestException as e:
        print(f'Error fetching JSearch data: {e}')
        return []
    
def iter_jsearch_jobs(city, role, max_pages=1):
    """Iterate through all pages of jobs for a given (city, role) pair"""
    for page in range(1, max_pages + 1):
        jobs = fetch_jsearch_page(city, role, page)
        if not jobs:
            break
        yield from jobs
        time.sleep(0.5)
        
def normalize_jsearch_job(job):
    """Normalize job data for insertion"""
    return {
        'source': 'jsearch',
        'job_id': job.get('job_id', ''),
        'fetched_at': date.today(),
        'payload': json.dumps(job)
        }
    
def insert_jsearch_rows(rows):
    """Insert rows into the jsearch_jobs table"""
    if not rows:
        return
    meta = MetaData()
    table = Table('jsearch_jobs', meta, autoload_with=get_engine(), schema='bronze')
    stmt = pg_insert(table).values(rows).on_conflict_do_nothing(index_elements=['job_id', 'fetched_at'])
    with get_engine().begin() as conn:
        conn.execute(stmt)
        
def ingest_jsearch(city, role, max_pages=1):
    jobs = [normalize_jsearch_job(j) for j in iter_jsearch_jobs(city, role, max_pages)]
    insert_jsearch_rows(jobs)
    print(f'Ingested {len(jobs)} JSearch jobs for {city} and {role}')
    
if __name__ == '__main__':
    # test the function 
    ingest_jsearch('Chicago, IL', 'data analyst', max_pages=1)