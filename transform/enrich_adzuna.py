"""
This is the main enrichment script that will take one raw job from jobs_raw.payload and return a cleaned and structured version of it that can be inserted into jobs_clean. 
Uses utils.py to extract derived fields, apply logic for is_remote, industry, etc., and returns a clean python dictrionary. 
"""
from transform.utils import get_is_remote, get_industry, get_job_type, get_yoe, get_education, categorize_seniority, get_cbsa_code
from datetime import datetime, timezone
import json
import pandas as pd
from database.db import get_engine

# def enrich_job(raw_job: dict, raw_id: int) -> dict:
#     """
#     Take one raw job from jobs_raw.payload and return a cleaned and structured version of it that can be inserted into jobs_clean. 
#     """
    
#     us_state_abbrev = {
#     'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
#     'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
#     'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
#     'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
#     'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
#     'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
#     'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
#     'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
#     'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND',
#     'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA',
#     'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
#     'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
#     'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
#     'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia': 'DC',
#     }
    
#     title = raw_job.get("title", '')
#     description = raw_job.get('description', '')
#     location = raw_job.get('location', {}).get('display_name', '')
#     area = raw_job.get('location', {}).get('area', [])
    
#     city = area[-1] if area else ''
#     county = area[2] if area else ''
#     state_full = area[1] if area else ''
#     state_code = us_state_abbrev.get(state_full, '')
    
#     company = raw_job.get('company', {}).get('display_name', '')
#     category = raw_job.get('category', {}).get('label', '')
#     salary_min = raw_job.get('salary_min')
#     salary_max = raw_job.get('salary_max')
#     created = raw_job.get('created')
    
#     return {
#         'raw_id': raw_id,
#         'source': 'adzuna',
#         'title': title,
#         'location': location,
#         'company': company,
#         'category': category,
#         'description': description,
#         'created': created,
#         'salary_min': salary_min,
#         'salary_max': salary_max,
#         'city': city,
#         'county': county,
#         'state': state_code,
#         'seniority': categorize_seniority(title, description),
#         'is_remote': get_is_remote(title, description),
#         'industry': get_industry(title, company, category),
#         'job_type': get_job_type(description),
#         'yoe_min': get_yoe(description),
#         'education': get_education(description),
#         'cbsa_code': get_cbsa_code(f"{city}, {state_code}")
#     }
    
def enrich_adzuna(payload, job_id):
    """Enrich a single Adzuna job"""
    
    us_state_abbrev = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND',
    'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA',
    'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY', 'District of Columbia': 'DC',
    }
    
    job = json.loads(payload)
    
    title = job.get('title', '')
    description = job.get('description', '')
    location = job.get('location', {}).get('display_name', '')
    area = job.get('location', {}).get('area', [])
    
    city = area[-1] if area else ''
    county = area[2] if area else ''
    state_full = area[1] if area else ''
    state_code = us_state_abbrev.get(state_full, '')
    
    company = job.get('company', {}).get('display_name', '')
    category = job.get('category', {}).get('label', '')
    salary_min = job.get('salary_min')
    salary_max = job.get('salary_max')
    created = job.get('created')
    
    return {
        'source': 'adzuna',
        'job_id': job_id,
        
        'title': title,
        'company': company,
        'location': location,
        'city': city,
        'county': county,
        'state': state_code,
        'cbsa_code': get_cbsa_code(f"{city}, {state_code}"),
        'category': category,
        'description': description,
        'post_date': created,
        'url': job.get('redirect_url', ''),
        
        'salary_min': salary_min,
        'salary_max': salary_max,
        
        'seniority': categorize_seniority(title, description),
        'is_remote': get_is_remote(title, description),
        'industry': get_industry(title, company, category),
        'job_type': get_job_type(description),
        'yoe_min': get_yoe(description),
        'education': get_education(description),
        
        'processed_at': datetime.now(timezone.utc)
    }
    
# def run_adzuna_enrichment():
#     """Run enrichment for all Adzuna jobs in the database"""
#     query = """
#     SELECT b.job_id, b.payload
#     FROM bronze.adzuna_jobs b
#     LEFT JOIN silver.jobs s ON s.source = 'adzuna' AND s.job_id = b.job_id
#     WHERE s.job_id IS NULL
#     """
    
#     df_raw = pd.read_sql(query, get_engine())
    
#     if df_raw.empty:
#         print('No new jobs to enrich')
#         return
    
#     enriched = [enrich_adzuna(row.payload, row.job_id) for _, row in df_raw.iterrows()]
#     df_enriched = pd.DataFrame(enriched)
    
#     df_enriched.to_sql('jobs', get_engine(), schema='silver', if_exists='append', index=False)
#     print(f'Inserted {len(df_enriched)} rows into silver.jobs')
    
# In transform/enrich_adzuna.py
def run_adzuna_enrichment():
    """Run enrichment - keep only latest version of each job in silver"""
    
    # Get the LATEST version of each job that's not already in silver
    query = """
    WITH latest_jobs AS (
        SELECT DISTINCT ON (job_id) 
            job_id, 
            payload,
            fetched_at
        FROM bronze.adzuna_jobs 
        ORDER BY job_id, fetched_at DESC
    )
    SELECT lj.job_id, lj.payload, lj.fetched_at
    FROM latest_jobs lj
    LEFT JOIN silver.jobs s ON s.source = 'adzuna' AND s.job_id = lj.job_id
    WHERE s.job_id IS NULL
    """
    
    df_raw = pd.read_sql(query, get_engine())
    
    if df_raw.empty:
        print('No new Adzuna jobs to enrich')
        return
    
    print(f"Found {len(df_raw)} unique jobs to process (latest versions)")
    
    enriched = [enrich_adzuna(row.payload, row.job_id) for _, row in df_raw.iterrows()]
    df_enriched = pd.DataFrame(enriched)
    
    df_enriched.to_sql('jobs', get_engine(), schema='silver', if_exists='append', index=False)
    print(f'✅ Inserted {len(df_enriched)} rows into silver.jobs')

if __name__ == '__main__':
    run_adzuna_enrichment()