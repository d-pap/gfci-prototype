from transform.utils import get_is_remote, get_industry, get_job_type, get_yoe, get_education, categorize_seniority, get_cbsa_code, standardize_job_type
from datetime import datetime, timezone, date
import json
import pandas as pd
from database.db import get_engine

def enrich_jsearch(payload, job_id):
    """Enrich JSearch jobs"""
    
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
    
    title = job.get('job_title', '')
    description = job.get('job_description', '')
    company = job.get('employer_name', '')
    
    location = job.get('job_location', '')
    city = job.get('job_city', '')
    state_full = job.get('job_state', '')
    
     # Convert full state name to abbreviation
    state_code = us_state_abbrev.get(state_full, state_full)  # Fallback to original if not found
    
    # Handle edge cases
    if not state_code and location:
        # Try to extract from job_location as backup
        # "Chicago, IL" -> extract "IL"
        location_parts = location.split(', ')
        if len(location_parts) >= 2:
            potential_state = location_parts[-1].strip()
            if len(potential_state) == 2:  # Looks like abbreviation
                state_code = potential_state
    
    category = job.get('employer_company_type', '')
    url = job.get('job_apply_link', '')
    salary_min = job.get('job_min_salary', '')
    salary_max = job.get('job_max_salary', '')
    post_date = job.get('job_posted_at_datetime_utc', '') or date.today()
    raw_job_type = job.get('job_employment_type', '')
    
    standard_job_type = standardize_job_type(raw_job_type, title, description, 'jsearch')
    
    return {
        'source': 'jsearch',
        'job_id': job_id,
        
        'title': title,
        'company': company,
        'location': location,
        'city': city,
        'county': '',  # JSearch might not provide county
        'state': state_code,
        'cbsa_code': get_cbsa_code(f"{city}, {state_code}"),
        'category': category,
        'description': description,
        'post_date': post_date,
        'url': url,
        
        'salary_min': salary_min,
        'salary_max': salary_max,
        
        'seniority': categorize_seniority(title, description),
        'is_remote': get_is_remote(title, description),
        'industry': get_industry(title, company, ''),
        'job_type': standard_job_type,
        'yoe_min': get_yoe(description),
        'education': get_education(description),
        
        'processed_at': datetime.now(timezone.utc)
    }

def run_jsearch_enrichment():
    """Run enrichment for all JSearch jobs in the database"""
    query = """
    SELECT b.job_id, b.payload
    FROM bronze.jsearch_jobs b
    LEFT JOIN silver.jobs s ON s.source = 'jsearch' AND s.job_id = b.job_id
    WHERE s.job_id IS NULL
    """
    
    df_raw = pd.read_sql(query, get_engine())
    
    if df_raw.empty:
        print('No new JSearch jobs to enrich')
        return
    
    enriched = [enrich_jsearch(row.payload, row.job_id) for _, row in df_raw.iterrows()]
    df_enriched = pd.DataFrame(enriched)
    
    df_enriched.to_sql('jobs', get_engine(), schema='silver', if_exists='append', index=False)
    print(f'Inserted {len(df_enriched)} JSearch rows into silver.jobs')
    
if __name__ == '__main__':
    run_jsearch_enrichment()