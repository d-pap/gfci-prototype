import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def get_engine():
    """Get database engine with environment variables"""
    load_dotenv()
    PG_USER = os.getenv("PG_USER")
    PG_PASSWORD = os.getenv("PG_PASSWORD")
    PG_PORT = os.getenv("PG_PORT")
    PG_DB = os.getenv("PG_DB")
    
    # Check that we have the required variables
    if not all([PG_USER, PG_PASSWORD, PG_DB]):
        raise ValueError("Missing required database environment variables (PG_USER, PG_PASSWORD, PG_DB)")
    
    # Create and return database engine
    CONN_STR = f"postgresql://{PG_USER}:{PG_PASSWORD}@localhost:{PG_PORT}/{PG_DB}"
    engine = create_engine(CONN_STR, isolation_level="AUTOCOMMIT")
    return engine

def run_sql_file(engine, sql_file_path):
    """
    Execute a SQL file against the database
    Used for running schema files and enrichment scripts
    """
    with open(sql_file_path, "r") as file:
        sql_content = file.read()
    
    with engine.begin() as connection:
        connection.execute(text(sql_content))
    
    print(f"Successfully executed: {sql_file_path}")

def init_schema():
    """Initialize the database by running the init_schema.sql file"""
    engine = get_engine()
    run_sql_file(engine, "sql/0_init_schema.sql")
    
def create_tables():
    """Create all tables in the database"""
    engine = get_engine()
    run_sql_file(engine, "sql/1_create_tables.sql")

def run_sql_query(engine, query):
    """
    Execute a single SQL query and return results
    Used for analysis queries
    """
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return result.fetchall() 
