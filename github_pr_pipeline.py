import requests
import pandas as pd
from datetime import datetime, timedelta
import pyodbc
import logging
from typing import List, Dict
import os
#from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config - Replace with your values or env vars
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')  # PAT
SYNAPSE_SERVER = os.getenv('SYNAPSE_SERVER')
SYNAPSE_DB = os.getenv('SYNAPSE_DB')
SYNAPSE_USER = os.getenv('SYNAPSE_USER')
SYNAPSE_PASS = os.getenv('SYNAPSE_PASS')
#REPOS = ['octocat/Hello-World', 'pallets/flask', 'microsoft/TypeScript']  # Provided repos; add health ones like 'WHO/world-health-data'
REPOS = ['Brahma6/testing_repo']  # For testing
TABLE_NAME = 'github_pull_requests'
DAYS_BACK = 30  # Historical

headers = {'Authorization': f'token {GITHUB_TOKEN}', 'Accept': 'application/vnd.github.v3+json'}

def fetch_prs(repo: str, since: str = None) -> List[Dict]:
    """Fetch PRs for repo, paginated, optional since date (ISO format)."""
    owner, repo_name = repo.split('/')
    url = f'https://api.github.com/repos/{owner}/{repo_name}/pulls?state=all&sort=updated&direction=desc&per_page=100'
    if since:
        url += f'&since={since}'
    all_prs = []
    page = 1
    while True:
        page_url = f'{url}&page={page}'
        resp = requests.get(page_url, headers=headers)
        if resp.status_code != 200:
            logger.error(f'Error {resp.status_code} for {repo}: {resp.text}')
            break
        prs = resp.json()
        if not prs:
            break
        all_prs.extend(prs)
        page += 1
        logger.info(f'Fetched {len(prs)} PRs from page {page} for {repo}')
    return all_prs

def clean_pr_data(prs: List[Dict]) -> pd.DataFrame:
    """Clean/transform: handle nulls, parse dates, dedupe, add metrics."""
    df = pd.DataFrame(prs)
    if df.empty:
        return df
    
    # Rename columns
    df = df.rename(columns={'html_url': 'url', 'user': 'user_obj'})
    
    # Extract author from nested user object if it exists
    if 'user_obj' in df.columns:
        df['author'] = df['user_obj'].apply(lambda x: x.get('login') if isinstance(x, dict) else None)
    else:
        df['author'] = None
    
    # Add repo_full_name if not exists
    if 'repo_full_name' not in df.columns:
        df['repo_full_name'] = REPOS[0]

    # Convert dates
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'])
    if 'updated_at' in df.columns:
        df['updated_at'] = pd.to_datetime(df['updated_at'])
    if 'closed_at' in df.columns:
        df['closed_at'] = pd.to_datetime(df['closed_at'])
    if 'merged_at' in df.columns:
        df['merged_at'] = pd.to_datetime(df['merged_at'])

    # Calculate merge_days
    if 'merged_at' in df.columns and 'created_at' in df.columns:
        df['merge_days'] = (df['merged_at'] - df['created_at']).dt.days.where(df['merged_at'].notna(), 0.0)
    else:
        df['merge_days'] = 0.0
    
    # Add ingested_at
    df['ingested_at'] = datetime.now()
    
    # Select and order columns - only include columns that exist
    cols = ['id', 'number', 'repo_full_name', 'title', 'state', 'locked', 'created_at', 'updated_at', 'closed_at', 'merged_at','author', 'merge_days', 'ingested_at']
    
    # Only keep columns that exist in the dataframe
    cols = [col for col in cols if col in df.columns]
    df = df[cols]
    
    # Clean: drop dups, null titles, fillna
    df = df.drop_duplicates(subset=['id']).dropna(subset=['title']).fillna({'additions': 0, 'deletions': 0})
    # print df columns
    logger.info(f'DataFrame columns: {df.columns.tolist()}')
    logger.info(f'DataFrame shape: {df.shape}')
    print(df.head(10))
    #logger.info(f'Cleaned PR data:\n{df.to_string()}')
    #print(df.info())
    return df

def load_to_synapse(df: pd.DataFrame, conn_str: str):
    """Append to Synapse table; create if missing."""
    print("Inside load_to_synapse function")
    
    # Check available ODBC drivers
    available_drivers = pyodbc.drivers()
    logger.info(f'Available ODBC drivers: {available_drivers}')
    
    # Find the appropriate SQL Server driver - prioritize ODBC Driver 18
    driver_name = None
    for driver in ['ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server', 'ODBC Driver 13 for SQL Server', 'SQL Server']:
        if driver in available_drivers:
            driver_name = driver
            logger.info(f'Using ODBC driver: {driver_name}')
            break
    
    if not driver_name:
        logger.error(f'No SQL Server ODBC driver found. Available drivers: {available_drivers}')
        logger.warning('Skipping Synapse load. Please install ODBC Driver 18 for SQL Server.')
        return
    
    # Rebuild connection string with available driver
    conn_str = f'DRIVER={{{driver_name}}};SERVER={SYNAPSE_SERVER};DATABASE={SYNAPSE_DB};UID={SYNAPSE_USER};PWD={SYNAPSE_PASS};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    
    try:
        conn = pyodbc.connect(conn_str)
        conn.autocommit = True  # Required for DDL statements in Azure Synapse
        cursor = conn.cursor()
        
        # Create table if not exists - use HEAP table (no columnstore index) for Azure Synapse compatibility
        # NVARCHAR(MAX) is not supported in columnstore indexes
        create_sql = f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{TABLE_NAME}' AND xtype='U')
        CREATE TABLE {TABLE_NAME} (
            id BIGINT NOT NULL,
            number INT,
            repo_full_name VARCHAR(255),
            title NVARCHAR(MAX),
            state VARCHAR(20),
            locked BIT,
            created_at DATETIME2,
            updated_at DATETIME2,
            closed_at DATETIME2,
            merged_at DATETIME2,
            author VARCHAR(255),
            merge_days FLOAT,
            ingested_at DATETIME2,
            CONSTRAINT pk_github_pull_requests PRIMARY KEY NONCLUSTERED (id) NOT ENFORCED
        )
        WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
        """
        cursor.execute(create_sql)
        
        # Disable autocommit for data operations
        conn.autocommit = True
        
        # Insert data row by row to avoid issues
        for _, row in df.iterrows():
            insert_sql = f"""
            INSERT INTO {TABLE_NAME} (id, number, repo_full_name, title, state, locked, created_at, updated_at, closed_at, merged_at, author, merge_days, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            values = tuple(row)
            print("values:", values)
            try:
                cursor.execute(insert_sql, values)
            except pyodbc.IntegrityError as e:
                logger.warning(f'Duplicate or constraint error for PR {row["id"]}: {e}')
        
        conn.commit()
        conn.close()
        logger.info(f'Loaded {len(df)} rows to {TABLE_NAME}')
    except Exception as e:
        logger.error(f'Error connecting to Synapse: {e}')
        logger.warning('Skipping Synapse load due to connection error.')

def main():
    """Daily ETL: historical 30d + new."""
    #load_dotenv()  # reads .env
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    print()
    since_date = (datetime.now() - timedelta(days=DAYS_BACK)).isoformat() + 'Z'
    all_data = []
    for repo in REPOS:
        prs = fetch_prs(repo, since_date)
        logger.info(f'Processed {len(prs)} PRs for {repo}')
        df_repo = clean_pr_data(prs)
        all_data.append(df_repo.assign(repo_full_name=repo))
        logger.info(f'Cleaned data has {len(df_repo)} PRs for {repo}')
       
    if all_data:
        df_final = pd.concat(all_data, ignore_index=True)
        # Dedupe across repos
        df_final = df_final.drop_duplicates(subset=['id'])
    
        print(f'Total PRs to load: {len(df_final)}')
        logger.info(f'Total PRs to load: {len(df_final)}')
        load_to_synapse(df_final, None)  # conn_str built inside function
    else:
        logger.warning('No data fetched')

if __name__ == '__main__':
    main()
