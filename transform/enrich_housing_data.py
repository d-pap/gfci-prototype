import pandas as pd
from database.db import get_engine
from pathlib import Path

# def load_zori(csv_path: Path):
#     """
#     Read the zillow ZORI csv and insert latest rent value into zillow_zori table
#     Only keeps metro rows (RegionType == 'msa')
#     """
    
#     # read csv
#     df = pd.read_csv(csv_path)
    
#     # keep metros only
#     df = df[df['RegionType'] == 'msa']
    
#     # get latest date column
#     latest_date = df.columns[-1]
    
#     # select and rename columns
#     keep = df[['RegionID', 'SizeRank', 'RegionName', 'StateName', latest_date]].copy()
#     keep = keep.rename(columns={
#         'RegionID': 'region_id', 
#         'SizeRank': 'size_rank',
#         'RegionName': 'region_name', 
#         'StateName': 'state_code',
#         latest_date: 'zori_latest'
#     })
    
#     # load to postgres table 
#     engine = get_engine()
#     keep.to_sql('zillow_zori', engine, if_exists='append', index=False)
#     print(f'Inserted {len(keep)} rows into zillow_zori')
    
# def enrich_zori():
#     """Enrich ZORI data and store in silver.zori_rents table"""
    
#     query = """
#     SELECT b.
#     """

########################################################################################
def enrich_zori():
    """read raw zillow zori csv from bronze, clean, enrich, and store in silver"""
    
    df = pd.read_sql("SELECT * FROM bronze.zillow_zori", con=get_engine())
    
    # keep only metro areas
    df = df[df['RegionType'] == 'msa']
    
    # get latest date column (dynamic)
    
    # method 1: get last column
    # latest_date = df.columns[-1]
    
    # method 2: get all date columns and find the latest
    # date_columns = [col for col in df.columns if col not in ['RegionID', 'SizeRank', 'RegionName', 'StateName', 'RegionType']]
    # latest_date = max(date_columns)
    
    # method 3: get all date columns formatted as YYYY-MM-DD and get latest
    date_cols = [col for col in df.columns if '-' in col and len(col) == 10]
    latest_date = max(date_cols)
    
    # select and standardize columns
    clean_df = df[['RegionID', 'SizeRank', 'RegionName', 'StateName', latest_date]].copy()
    clean_df = clean_df.rename(columns={
        'RegionID': 'region_id',
        'SizeRank': 'size_rank',
        'RegionName': 'region_name',
        'StateName': 'state_code',
        latest_date: 'metric_value_latest'
    })
    
    # add metadata - data source, metric type, metric value latest, date recorded, processed at
    clean_df['data_source']='zillow_zori'
    clean_df['metric_type'] = 'rent_index_latest'
    clean_df['date_recorded'] = latest_date
    clean_df['processed_at'] = pd.Timestamp.now()
    
    # store in silver schema: silver.housing_metrics
    clean_df.to_sql(
        name='housing_metrics',
        con=get_engine(),
        schema='silver',
        if_exists='append',
        index=False
    )
    print(f'Inserted {len(clean_df)} rows into silver.housing_metrics')
    

def enrich_zhvi():
    """read raw zillow zhvi csv from bronze, clean, enrich, and store in silver"""
    
    pass
