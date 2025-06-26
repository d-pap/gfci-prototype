"""
Migration script to set up v2 schema
Run this to create new tables and test the migration
"""
import sys
from pathlib import Path

# Add the parent directory to Python path so we can import from database, etc.
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from database.db import get_engine, run_sql_file

def create_v2_schema():
    """Create the new v2 tables"""
    print("Creating v2 schema tables...")
    try:
        run_sql_file(get_engine(), "sql/2_create_tables_v2.sql")
        print("✅ V2 schema created successfully!")
    except Exception as e:
        print(f"❌ Error creating v2 schema: {e}")
        sys.exit(1)

def test_bronze_tables():
    """Test that the new bronze tables work correctly"""
    from datetime import date
    from sqlalchemy import text
    
    engine = get_engine()
    print("\nTesting bronze tables...")
    
    try:
        with engine.begin() as conn:
            # Test job_calls table
            conn.execute(text("""
                INSERT INTO bronze.job_calls (run_date, city, role, page, api_count, api_mean_salary)
                VALUES (:run_date, :city, :role, :page, :api_count, :api_mean_salary)
            """), {
                'run_date': date.today(),
                'city': 'Test City',
                'role': 'test role',
                'page': 1,
                'api_count': 100,
                'api_mean_salary': 75000
            })
            print("✅ bronze.job_calls table works!")
            
            # Test raw_jobs table
            conn.execute(text("""
                INSERT INTO bronze.raw_jobs (job_id, source, first_seen, last_seen, payload)
                VALUES (:job_id, :source, :first_seen, :last_seen, :payload)
                ON CONFLICT (source, job_id) DO UPDATE 
                SET last_seen = EXCLUDED.last_seen,
                    times_seen = bronze.raw_jobs.times_seen + 1
            """), {
                'job_id': 'test-123',
                'source': 'adzuna',
                'first_seen': date.today(),
                'last_seen': date.today(),
                'payload': '{"test": "data"}'
            })
            print("✅ bronze.raw_jobs table works!")
            
            # Clean up test data
            conn.execute(text("DELETE FROM bronze.job_calls WHERE city = 'Test City'"))
            conn.execute(text("DELETE FROM bronze.raw_jobs WHERE job_id = 'test-123'"))
            
    except Exception as e:
        print(f"❌ Error testing bronze tables: {e}")
        sys.exit(1)

def verify_dual_write():
    """Verify that dual-write to old and new schemas works"""
    from sqlalchemy import text
    
    print("\nTesting dual-write functionality...")
    
    try:
        # Import and test the new ingest function
        from ingest.ingest_adzuna_v2 import ingest_adzuna_v2
        
        # Run a small test (1 page only)
        print("Fetching 1 page of test data...")
        count = ingest_adzuna_v2("Detroit, MI", "data analyst", max_pages=1)
        
        if count > 0:
            print(f"✅ Successfully ingested {count} jobs")
            
            # Verify data in both schemas
            engine = get_engine()
            with engine.begin() as conn:
                # Check new schema
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM bronze.raw_jobs 
                    WHERE source = 'adzuna' AND last_seen = CURRENT_DATE
                """))
                new_count = result.scalar()
                
                # Check old schema
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM bronze.adzuna_jobs 
                    WHERE fetched_at::date = CURRENT_DATE
                """))
                old_count = result.scalar()
                
                print(f"  - New schema (bronze.raw_jobs): {new_count} jobs")
                print(f"  - Old schema (bronze.adzuna_jobs): {old_count} jobs")
                
                # Check job_calls
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM bronze.job_calls 
                    WHERE run_date = CURRENT_DATE
                """))
                call_count = result.scalar()
                print(f"  - API calls recorded: {call_count}")
                
        else:
            print("⚠️  No jobs fetched - API might be down or credentials missing")
            
    except Exception as e:
        print(f"❌ Error testing dual-write: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run the migration"""
    print("=== Starting V2 Schema Migration ===\n")
    
    # Step 1: Create new schema
    create_v2_schema()
    
    # Step 2: Test bronze tables
    test_bronze_tables()
    
    # Step 3: Test dual-write
    response = input("\nDo you want to test dual-write with real API data? (y/n): ")
    if response.lower() == 'y':
        verify_dual_write()
    
    print("\n=== Migration Complete ===")
    print("\nNext steps:")
    print("1. Update your main.py to import from ingest.ingest_adzuna_v2")
    print("2. Create enrichment functions for silver.jobs_v2")
    print("3. Create aggregation functions for gold schema")
    print("4. Once stable, stop writing to old schema")

if __name__ == "__main__":
    main() 