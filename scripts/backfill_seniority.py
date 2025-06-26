"""
Backfill script to update seniority classifications in silver.jobs_v2
Uses improved logic combining title keywords, salary thresholds, and description analysis
"""
# System imports first
import sys
import os
import pandas as pd

# Path setup BEFORE project imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Project imports AFTER path setup
from database.db import get_engine
from sqlalchemy import text
from transform.utils import categorize_role

def backfill_all_seniority(dry_run=True):
    # 1. Get all jobs from silver.jobs_v2
    # 2. For each job, calculate new seniority using improved logic
    # 3. Compare old vs new seniority
    # 4. If dry_run=True: show what would change
    # 5. If dry_run=False: update the database
    # 6. Report statistics on changes made
    """
    Update seniority classifications for all jobs in silver.jobs_v2
    
    Args:
        dry_run (bool): If True, show what would change without updating database
    """
    
    print("🔄 Starting seniority backfill process...")
    
    engine = get_engine()
    
    # Step 1: Get all jobs from silver.jobs_v2
    print("📊 Fetching all jobs from silver.jobs_v2...")
    
    query = """
    SELECT 
        source, job_id, title, description, salary_min, salary_max, 
        city, seniority as current_seniority
    FROM silver.jobs_v2
    ORDER BY job_id
    """
    
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("❌ No jobs found in silver.jobs_v2")
        return
    
    print(f"📈 Found {len(df)} jobs to process")
    
    # Step 2: Calculate new seniority for each job
    print("🧠 Calculating new seniority classifications...")
    
    changes = {
        'jr → mid': 0,
        'jr → sr': 0,
        'mid → jr': 0,
        'mid → sr': 0,
        'sr → jr': 0,
        'sr → mid': 0,
        'no_change': 0,
        'null_to_value': 0
    }
    
    jobs_to_update = []
    
    for idx, row in df.iterrows():
        # Show progress every 100 jobs
        if idx % 100 == 0:
            print(f"  Processed {idx}/{len(df)} jobs...")
        
        # Calculate new seniority
        new_seniority = categorize_role(
            title=row['title'],
            description=row['description'],
            salary_min=row['salary_min'],
            salary_max=row['salary_max'],
            city=row['city']
        )
        
        current_seniority = row['current_seniority']
        
        # Track changes
        if current_seniority is None:
            changes['null_to_value'] += 1
            jobs_to_update.append((row['source'], row['job_id'], new_seniority))
        elif current_seniority != new_seniority:
            change_key = f"{current_seniority} → {new_seniority}"
            if change_key in changes:
                changes[change_key] += 1
            jobs_to_update.append((row['source'], row['job_id'], new_seniority))
        else:
            changes['no_change'] += 1
    
    # Step 3: Report what would change
    print("\n📊 CHANGE SUMMARY:")
    print(f"{'Change Type':<15} {'Count':<8}")
    print("-" * 25)
    for change_type, count in changes.items():
        if count > 0:
            print(f"{change_type:<15} {count:<8}")
    
    total_changes = len(jobs_to_update)
    print(f"\n🎯 Total jobs to update: {total_changes}")
    
    if total_changes == 0:
        print("✅ No changes needed - all seniority classifications are already optimal!")
        return
    
    # Step 4: Show sample changes if dry run
    if dry_run:
        print("\n🔍 SAMPLE CHANGES (first 10):")
        print(f"{'Job ID':<15} {'Title':<40} {'Old':<8} {'New':<8}")
        print("-" * 75)
        
        sample_count = 0
        for idx, row in df.iterrows():
            if sample_count >= 10:
                break
                
            new_seniority = categorize_role(
                title=row['title'],
                description=row['description'],
                salary_min=row['salary_min'],
                salary_max=row['salary_max'],
                city=row['city']
            )
            
            if row['current_seniority'] != new_seniority:
                job_id = row['job_id'][:12] + "..." if len(row['job_id']) > 15 else row['job_id']
                title = row['title'][:37] + "..." if len(row['title']) > 40 else row['title']
                old_val = row['current_seniority'] or 'NULL'
                
                print(f"{job_id:<15} {title:<40} {old_val:<8} {new_seniority:<8}")
                sample_count += 1
        
        print(f"\n🚫 DRY RUN MODE - No changes made to database")
        print("💡 Run with dry_run=False to apply these changes")
        return
    
    # Step 5: Apply changes to database
    print(f"\n💾 Updating {total_changes} jobs in database...")
    
    batch_size = 100
    updated_count = 0
    
    with engine.begin() as conn:
        for i in range(0, len(jobs_to_update), batch_size):
            batch = jobs_to_update[i:i + batch_size]
            
            for source, job_id, new_seniority in batch:
                conn.execute(text("""
                    UPDATE silver.jobs_v2 
                    SET seniority = :seniority,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE source = :source AND job_id = :job_id
                """), {
                    'source': source,
                    'job_id': job_id,
                    'seniority': new_seniority
                })
                updated_count += 1
            
            print(f"  Updated {min(updated_count, len(jobs_to_update))}/{len(jobs_to_update)} jobs...")
    
    print(f"\n✅ Successfully updated {updated_count} jobs!")
    print("🎉 Seniority backfill complete!")

if __name__ == "__main__":
    #! Test with dry run first
    print("Running in DRY RUN mode...")
    backfill_all_seniority(dry_run=True)
    
    #! Uncomment to actually apply changes
    # print("\nRunning ACTUAL UPDATE...")
    # backfill_all_seniority(dry_run=False)