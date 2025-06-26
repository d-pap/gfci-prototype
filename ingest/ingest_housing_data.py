"""
Housing data loader for CSV files (Zillow)

ZORI:
- https://www.zillow.com/research/data/ and more details here: https://www.zillow.com/research/methodology-zori-repeat-rent-27092/
- what it is: 
    - A smoothed measure of the typical observed market rate rent across a given region. 
    - ZORI is a repeat-rent index that is weighted to the rental housing stock to ensure representativeness across the entire market, not just those homes currently listed for-rent. 
    - The index is dollar-denominated by computing the mean of listed rents that fall into the 35th to 65th percentile range for all homes and apartments in a given region, which is weighted to reflect the rental housing stock.
- use it for: estimate cost of renting in each area

--- OTHER CSVs TO CONSIDER: ---
ZHVI:
- https://www.zillow.com/research/data/
- what it is: median home value for a given month/year 
    - A measure of the typical home value and market changes across a given region and housing type. 
    - It reflects the typical value for homes in the 35th to 65th percentile range. 
    - Available as a smoothed, seasonally adjusted measure and as a raw measure.
- use it for: estimate cost of buying a home in each area

New Renter Income Needed:
- https://www.zillow.com/research/data/
- what it is: 
    - An estimate of the household income required to spend less than 30% of monthly income to newly lease the typical rental.
- use it for: estimate income needed to afford a home in each area

New Renter Affordability:
- https://www.zillow.com/research/data/
- what it is: 
    - A measure of the share of income the median household would spend to newly lease the typical rental. 
    - Typically, spending more than 30% of income on housing is considered unaffordable.
- use it for: estimate affordability of renting in each area

Affordable Home Price:
- https://www.zillow.com/research/data/
- what it is: 
    - An estimate of the home price such that the total monthly payment on such a home would not exceed 30% of the median household’s monthly income with a 20% down payment.
- use it for: ???
"""
import pandas as pd
from database.db import get_engine
from pathlib import Path

# def fetch_zillow_zori(csv_path):
#     """Get ZORI data from CSV, insert into bronze.zillow_zori table
#         It overwrites the bronze.zillow_zori table and uses columns from the csv, NOT the columns we set in create_tables.sql
        
#         Run this whenever we get a new ZORI csv
#     """
    
#     df = pd.read_csv(csv_path)
#     df = df.rename(columns={
#         'RegionID': 'region_id',
#         'SizeRank': 'size_rank',
#         'RegionName': 'region_name',
#         'StateName': 'state_code'
#     })
    
#     #! USE 'replace' INSTEAD OF 'append' TO OVERWRITE THE COLUMNS WE SET IN create_tables.sql FOR THIS TABLE
#     #! IN THE broze SCHEMA 
#     df.to_sql(name='zillow_zori', con=get_engine(), schema='bronze', if_exists='replace', index=False)
#     print(f'Inserted {len(df)} rows into zillow_zori')
########################################################################################
# ingest raw zillow data and store in bronze schema
def ingest_zori_raw(csv_path:Path):
    """Store complete raw ZORI csv in bronze schema"""
    
    df = pd.read_csv(csv_path)
    
    # store ALL columns as-is in bronze.zillow_zori
    df.to_sql(
        name='zillow_zori',
        con=get_engine(),
        schema='bronze',
        if_exists='replace',
        index=False
    )
    print(f'Inserted {len(df)} raw ZORI rows into bronze.zillow_zori')

def ingest_zhvi_raw(csv_path: Path):
    """Store complete raw ZHVI csv in bronze schema"""
    df = pd.read_csv(csv_path)
    
    df.to_sql(
        name='zillow_zhvi',
        con=get_engine(),
        schema='bronze',
        if_exists='replace',
        index=False
    )
    print(f'Inserted {len(df)} raw ZHVI rows into bronze.zillow_zhvi')
    
#! generic function to ingest any zillow csv
def ingest_zillow_csv(csv_path: Path, table_name: str):
    """Generic function to ingest any zillow csv into the bronze schema"""
    
    df = pd.read_csv(csv_path)
    
    df.to_sql(
        name=table_name,
        con=get_engine(),
        schema='bronze',
        if_exists='replace',
        index=False
    )
    print(f'Inserted {len(df)} raw {table_name} rows into bronze.{table_name}')
    
########################################################################################
if __name__ == '__main__':
    ingest_zori_raw(Path('data/housing/zori-6-19-2025.csv'))
    