"""
Backfill job descriptions for existing jobs in silver.jobs_v2
This script updates the description column for jobs that already exist in silver.jobs_v2
but don't have descriptions populated.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from database.db import get_engine
from sqlalchemy import text

def backfill_job_descriptions():
    """
    Update description column in silver.jobs_v2 for existing jobs
    that are missing descriptions by pulling from bronze.raw_jobs
    """
    
    engine = get_engine()
    
    # First, check how many jobs need description backfill
    check_query = """
    SELECT COUNT(*) as jobs_needing_descriptions
    FROM silver.jobs_v2 s
    INNER JOIN bronze.raw_jobs r 
        ON s.source = r.source AND s.job_id = r.job_id
    WHERE s.source = 'adzuna'
        AND (s.description IS NULL OR s.description = '')
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(check_query))
        jobs_needing_descriptions = result.scalar()
    
    if jobs_needing_descriptions == 0:
        print('No jobs need description backfill')
        return
    
    print(f"Found {jobs_needing_descriptions} jobs that need description backfill")
    
    # Get jobs that need description backfill
    query = """
    SELECT 
        s.source,
        s.job_id,
        r.payload
    FROM silver.jobs_v2 s
    INNER JOIN bronze.raw_jobs r 
        ON s.source = r.source AND s.job_id = r.job_id
    WHERE s.source = 'adzuna'
        AND (s.description IS NULL OR s.description = '')
    """
    
    df_jobs = pd.read_sql(query, engine)
    
    if df_jobs.empty:
        print('No jobs found that need description backfill')
        return
    
    print(f"Processing {len(df_jobs)} jobs for description backfill")
    
    # Process each job and extract description from payload
    updated_count = 0
    failed_count = 0
    
    with engine.begin() as conn:
        for _, row in df_jobs.iterrows():
            try:
                # Extract description from JSON payload
                payload = row['payload']
                description = payload.get('description', '') if payload else ''
                
                # Update the description in silver.jobs_v2
                conn.execute(text("""
                    UPDATE silver.jobs_v2 
                    SET description = :description,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE source = :source AND job_id = :job_id
                """), {
                    'description': description,
                    'source': row['source'],
                    'job_id': row['job_id']
                })
                
                updated_count += 1
                
                # Progress indicator every 100 jobs
                if updated_count % 100 == 0:
                    print(f"Processed {updated_count} jobs...")
                    
            except Exception as e:
                print(f"Error processing job {row['job_id']}: {e}")
                failed_count += 1
                continue
    
    print(f"✅ Backfill complete!")
    print(f"   - Successfully updated: {updated_count} jobs")
    if failed_count > 0:
        print(f"   - Failed to update: {failed_count} jobs")
    
    # Verify the backfill worked
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) as remaining_jobs_without_descriptions
            FROM silver.jobs_v2
            WHERE source = 'adzuna'
                AND (description IS NULL OR description = '')
        """))
        remaining = result.scalar()
        
        result = conn.execute(text("""
            SELECT COUNT(*) as jobs_with_descriptions
            FROM silver.jobs_v2
            WHERE source = 'adzuna'
                AND description IS NOT NULL 
                AND description != ''
        """))
        with_descriptions = result.scalar()
    
    print(f"   - Jobs with descriptions: {with_descriptions}")
    print(f"   - Jobs still missing descriptions: {remaining}")

if __name__ == "__main__":
    backfill_job_descriptions() 