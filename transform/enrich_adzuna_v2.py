"""
V2 Enrichment for Adzuna jobs
Processes jobs from bronze.raw_jobs to silver.jobs_v2
Includes gold schema aggregations
"""
from transform.utils import get_is_remote, get_industry, get_job_type, get_yoe, get_education, categorize_seniority, get_cbsa_code
from datetime import datetime, timezone, date
import json
import pandas as pd
from database.db import get_engine
from sqlalchemy import text

US_STATE_ABBREV = {
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

def enrich_adzuna_job_v2(raw_job_record):
    """Enrich a single Adzuna job for v2 schema"""
    
    job = raw_job_record['payload']
    
    title = job.get('title', '')
    description = job.get('description', '')
    location = job.get('location', {})
    display_name = location.get('display_name', '')
    area = location.get('area', [])
    
    # Parse location hierarchy
    city = area[-1] if len(area) >= 4 else ''
    county = area[2] if len(area) >= 3 else ''
    state_full = area[1] if len(area) >= 2 else ''
    state_code = US_STATE_ABBREV.get(state_full, '')
    
    company = job.get('company', {}).get('display_name', '')
    category = job.get('category', {})
    category_tag = category.get('tag', '')
    category_label = category.get('label', '')
    
    salary_min = job.get('salary_min')
    salary_max = job.get('salary_max')
    created = job.get('created')
    
    # Parse created date
    post_date = None
    if created:
        try:
            post_date = datetime.fromisoformat(created.replace('Z', '+00:00')).date()
        except:
            pass
    
    # Get coordinates
    latitude = location.get('latitude')
    longitude = location.get('longitude')
    
    return {
        'source': 'adzuna',
        'job_id': raw_job_record['job_id'],
        
        # Basic fields
        'title': title,
        'company': company,
        'location': display_name,
        'city': city,
        'county': county,
        'state': state_full,
        'state_code': state_code,
        'cbsa_code': get_cbsa_code(f"{city}, {state_code}") if city and state_code else None,
        
        # Categories
        'category': category_tag,
        'category_label': category_label,
        
        # Compensation
        'salary_min': salary_min,
        'salary_max': salary_max,
        
        # Dates
        'post_date': post_date,
        'first_seen': raw_job_record['first_seen'],
        'last_seen': raw_job_record['last_seen'],
        'times_seen': raw_job_record['times_seen'],
        
        # Status - set based on whether it was seen today
        'is_active': raw_job_record['last_seen'] == date.today(),
        
        # URL
        'url': job.get('redirect_url', ''),
        
        # Coordinates
        'latitude': latitude,
        'longitude': longitude,
        
        # Derived fields
        'seniority': categorize_seniority(title, description),
        'is_remote': get_is_remote(title, description),
        'industry': get_industry(title, company, category_label),
        'job_type': get_job_type(description),
        'yoe_min': get_yoe(description),
        'education': get_education(description),
    }

def run_adzuna_enrichment_v2():
    """Process new/updated jobs from bronze.raw_jobs to silver.jobs_v2"""
    
    engine = get_engine()
    
    # First, mark all jobs as inactive (they'll be reactivated if seen today)
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE silver.jobs_v2 
            SET is_active = FALSE 
            WHERE source = 'adzuna' AND is_active = TRUE
        """))
    
    # Get jobs that need processing
    query = """
    SELECT 
        r.job_id,
        r.payload,
        r.first_seen,
        r.last_seen,
        r.times_seen
    FROM bronze.raw_jobs r
    LEFT JOIN silver.jobs_v2 s 
        ON s.source = r.source 
        AND s.job_id = r.job_id
    WHERE r.source = 'adzuna'
        AND (
            s.job_id IS NULL  -- New jobs
            OR r.last_seen > s.last_seen  -- Updated jobs
        )
    """
    
    df_raw = pd.read_sql(query, engine)
    
    if df_raw.empty:
        print('No new Adzuna jobs to enrich for v2')
        return
    
    print(f"Found {len(df_raw)} jobs to process for v2")
    
    # Enrich all jobs
    enriched = []
    for _, row in df_raw.iterrows():
        try:
            enriched_job = enrich_adzuna_job_v2(row.to_dict())
            enriched.append(enriched_job)
        except Exception as e:
            print(f"Error enriching job {row['job_id']}: {e}")
            continue
    
    if not enriched:
        print("No jobs successfully enriched")
        return
        
    df_enriched = pd.DataFrame(enriched)
    
    # Upsert into silver.jobs_v2
    with engine.begin() as conn:
        for _, job in df_enriched.iterrows():
            # Convert job to dict and remove NaN values
            job_dict = job.where(pd.notnull(job), None).to_dict()
            
            # Build the upsert query
            conn.execute(text("""
                INSERT INTO silver.jobs_v2 (
                    source, job_id, title, company, location, city, county, state, 
                    state_code, cbsa_code, category, category_label, salary_min, 
                    salary_max, post_date, first_seen, last_seen, times_seen, 
                    is_active, url, latitude, longitude, seniority, is_remote, 
                    industry, job_type, yoe_min, education
                ) VALUES (
                    :source, :job_id, :title, :company, :location, :city, :county, 
                    :state, :state_code, :cbsa_code, :category, :category_label, 
                    :salary_min, :salary_max, :post_date, :first_seen, :last_seen, 
                    :times_seen, :is_active, :url, :latitude, :longitude, :seniority, 
                    :is_remote, :industry, :job_type, :yoe_min, :education
                )
                ON CONFLICT (source, job_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    company = EXCLUDED.company,
                    location = EXCLUDED.location,
                    city = EXCLUDED.city,
                    county = EXCLUDED.county,
                    state = EXCLUDED.state,
                    state_code = EXCLUDED.state_code,
                    cbsa_code = EXCLUDED.cbsa_code,
                    category = EXCLUDED.category,
                    category_label = EXCLUDED.category_label,
                    salary_min = EXCLUDED.salary_min,
                    salary_max = EXCLUDED.salary_max,
                    post_date = EXCLUDED.post_date,
                    last_seen = EXCLUDED.last_seen,
                    times_seen = EXCLUDED.times_seen,
                    is_active = EXCLUDED.is_active,
                    url = EXCLUDED.url,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    seniority = EXCLUDED.seniority,
                    is_remote = EXCLUDED.is_remote,
                    industry = EXCLUDED.industry,
                    job_type = EXCLUDED.job_type,
                    yoe_min = EXCLUDED.yoe_min,
                    education = EXCLUDED.education,
                    updated_at = CURRENT_TIMESTAMP
            """), job_dict)
    
    print(f'✅ Processed {len(df_enriched)} jobs into silver.jobs_v2')
    
    # Update gold schema aggregations
    update_gold_aggregations()

def update_gold_aggregations():
    """Update gold schema aggregations after enrichment"""
    
    engine = get_engine()
    run_date = date.today()
    
    with engine.begin() as conn:
        # Update city_job_stats
        conn.execute(text("""
            INSERT INTO gold.city_job_stats (
                city, state, run_date, total_jobs, active_jobs, new_jobs, 
                expired_jobs, jobs_with_salary, avg_salary_min, avg_salary_max,
                median_salary_min, median_salary_max, remote_jobs, fulltime_jobs,
                junior_jobs, mid_jobs, senior_jobs
            )
            SELECT
                city,
                state_code as state,
                :run_date as run_date,
                COUNT(*) as total_jobs,
                COUNT(*) FILTER (WHERE is_active = true) as active_jobs,
                COUNT(*) FILTER (WHERE first_seen = :run_date) as new_jobs,
                COUNT(*) FILTER (WHERE is_active = false AND last_seen < :run_date) as expired_jobs,
                COUNT(*) FILTER (WHERE salary_min IS NOT NULL) as jobs_with_salary,
                AVG(salary_min) as avg_salary_min,
                AVG(salary_max) as avg_salary_max,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_min) as median_salary_min,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary_max) as median_salary_max,
                COUNT(*) FILTER (WHERE is_remote = true) as remote_jobs,
                COUNT(*) FILTER (WHERE job_type = 'full-time') as fulltime_jobs,
                COUNT(*) FILTER (WHERE seniority = 'jr') as junior_jobs,
                COUNT(*) FILTER (WHERE seniority = 'mid') as mid_jobs,
                COUNT(*) FILTER (WHERE seniority = 'sr') as senior_jobs
            FROM silver.jobs_v2
            WHERE source = 'adzuna'
            GROUP BY city, state_code
            ON CONFLICT (city, state, run_date) DO UPDATE SET
                total_jobs = EXCLUDED.total_jobs,
                active_jobs = EXCLUDED.active_jobs,
                new_jobs = EXCLUDED.new_jobs,
                expired_jobs = EXCLUDED.expired_jobs,
                jobs_with_salary = EXCLUDED.jobs_with_salary,
                avg_salary_min = EXCLUDED.avg_salary_min,
                avg_salary_max = EXCLUDED.avg_salary_max,
                median_salary_min = EXCLUDED.median_salary_min,
                median_salary_max = EXCLUDED.median_salary_max,
                remote_jobs = EXCLUDED.remote_jobs,
                fulltime_jobs = EXCLUDED.fulltime_jobs,
                junior_jobs = EXCLUDED.junior_jobs,
                mid_jobs = EXCLUDED.mid_jobs,
                senior_jobs = EXCLUDED.senior_jobs,
                created_at = CURRENT_TIMESTAMP
        """), {'run_date': run_date})
        
        # Update latest_city_snapshot
        conn.execute(text("""
            INSERT INTO gold.latest_city_snapshot (
                city, state, active_jobs, new_jobs_7d, new_jobs_30d,
                avg_salary, job_growth_rate_7d, job_growth_rate_30d,
                top_categories
            )
            SELECT
                j.city,
                j.state_code as state,
                COUNT(*) FILTER (WHERE j.is_active = true) as active_jobs,
                COUNT(*) FILTER (WHERE j.first_seen >= CURRENT_DATE - INTERVAL '7 days') as new_jobs_7d,
                COUNT(*) FILTER (WHERE j.first_seen >= CURRENT_DATE - INTERVAL '30 days') as new_jobs_30d,
                AVG((j.salary_min + j.salary_max) / 2) as avg_salary,
                -- Calculate growth rates
                CASE 
                    WHEN COUNT(*) FILTER (WHERE j.first_seen < CURRENT_DATE - INTERVAL '7 days') > 0
                    THEN (COUNT(*) FILTER (WHERE j.first_seen >= CURRENT_DATE - INTERVAL '7 days')::numeric / 
                          COUNT(*) FILTER (WHERE j.first_seen < CURRENT_DATE - INTERVAL '7 days')) * 100
                    ELSE 0
                END as job_growth_rate_7d,
                CASE 
                    WHEN COUNT(*) FILTER (WHERE j.first_seen < CURRENT_DATE - INTERVAL '30 days') > 0
                    THEN (COUNT(*) FILTER (WHERE j.first_seen >= CURRENT_DATE - INTERVAL '30 days')::numeric / 
                          COUNT(*) FILTER (WHERE j.first_seen < CURRENT_DATE - INTERVAL '30 days')) * 100
                    ELSE 0
                END as job_growth_rate_30d,
                -- Top 5 categories
                ARRAY(
                    SELECT category_label
                    FROM (
                        SELECT category_label, COUNT(*) as cnt
                        FROM silver.jobs_v2
                        WHERE city = j.city AND state_code = j.state_code
                            AND is_active = true AND category_label IS NOT NULL
                        GROUP BY category_label
                        ORDER BY cnt DESC
                        LIMIT 5
                    ) top_cats
                ) as top_categories
            FROM silver.jobs_v2 j
            WHERE j.source = 'adzuna'
            GROUP BY j.city, j.state_code
            ON CONFLICT (city, state) DO UPDATE SET
                active_jobs = EXCLUDED.active_jobs,
                new_jobs_7d = EXCLUDED.new_jobs_7d,
                new_jobs_30d = EXCLUDED.new_jobs_30d,
                avg_salary = EXCLUDED.avg_salary,
                job_growth_rate_7d = EXCLUDED.job_growth_rate_7d,
                job_growth_rate_30d = EXCLUDED.job_growth_rate_30d,
                top_categories = EXCLUDED.top_categories,
                last_updated = CURRENT_TIMESTAMP
        """))
        
    print("✅ Updated gold schema aggregations")

if __name__ == "__main__":
    run_adzuna_enrichment_v2() 