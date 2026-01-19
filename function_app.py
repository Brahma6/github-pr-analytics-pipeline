import logging
import azure.functions as func
import requests
import pandas as pd
from datetime import datetime, timedelta
import pyodbc
import os
import logging
from time import sleep
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = func.FunctionApp()

# Config from App Settings - Load GITHUB_TOKEN early for headers
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
DAYS_BACK = int(os.getenv('DAYS_BACK', 30))
TABLE_NAME = os.getenv('TABLE_NAME', 'github_pull_requests')
#REPOS = ['octocat/Hello-World', 'pallets/flask', 'microsoft/TypeScript']  # Provided repos; add health ones like 'WHO/world-health-data'
REPOS = ['Brahma6/testing_repo']  # For testing
#REPOS = ['microsoft/TypeScript']  # For testing

headers = {'Authorization': f'token {GITHUB_TOKEN}', 'Accept': 'application/vnd.github.v3+json'}

def create_session_with_retries():
    """Create a requests session with retry strategy for network resilience."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_prs(repo: str, since: str = None) :
    """Fetch PRs for repo, paginated, optional since date (ISO format)."""
    owner, repo_name = repo.split('/')
    url = f'https://api.github.com/repos/{owner}/{repo_name}/pulls?state=all&sort=updated&direction=desc&per_page=100'
    if since:
        url += f'&since={since}'
    all_prs = []
    page = 1
    session = create_session_with_retries()
    
    try:
        while True:
            page_url = f'{url}&page={page}'
            try:
                resp = session.get(page_url, headers=headers, timeout=30)
                if resp.status_code != 200:
                    logger.error(f'Error {resp.status_code} for {repo}: {resp.text}')
                    break
                prs = resp.json()
                if not prs:
                    break
                all_prs.extend(prs)
                page += 1
                logger.info(f'Fetched {len(prs)} PRs from page {page} for {repo}')
            except requests.exceptions.ChunkedEncodingError as e:
                logger.warning(f'ChunkedEncodingError on page {page} for {repo}: {e}. Retrying...')
                sleep(2)  # Wait before retry
                continue
            except requests.exceptions.RequestException as e:
                logger.error(f'Request error on page {page} for {repo}: {e}')
                break
    finally:
        session.close()
    
    return all_prs

def clean_pr_data(prs) -> pd.DataFrame:
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
    TABLE_NAME = os.getenv('TABLE_NAME', 'github_pull_requests')
    
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

@app.timer_trigger(schedule="25 4 * * * *", arg_name="myTimer", run_on_startup=True, 
                   use_monitor=False)  # Daily at 9AM UTC
def github_pr_pipeline(myTimer: func.TimerRequest) -> None:
    logging.info('GitHub PR Pipeline triggered.')
    
    # Synapse connection config
    synapse_server = os.getenv('SYNAPSE_SERVER')
    synapse_db = os.getenv('SYNAPSE_DB')
    synapse_user = os.getenv('SYNAPSE_USER')
    synapse_pass = os.getenv('SYNAPSE_PASS')
    
    if not synapse_server or not synapse_db:
        logger.error('SYNAPSE_SERVER or SYNAPSE_DB not configured')
        return
    
    # Build connection string based on available credentials
    # Use SQL authentication for local development, Managed Identity for Azure
    if synapse_user and synapse_pass:
        # SQL authentication for local/development
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={synapse_server};"
            f"DATABASE={synapse_db};"
            f"UID={synapse_user};"
            f"PWD={synapse_pass};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
    else:
        # Managed Identity for Azure
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={synapse_server};"
            f"DATABASE={synapse_db};"
            f"Authentication=ActiveDirectoryMsi;"
        )
    
    # Fetch & process (same logic as before)
    since_date = (datetime.utcnow() - timedelta(days=DAYS_BACK)).isoformat() + 'Z'
    all_data = []
    
    for repo in REPOS:
        prs = fetch_prs(repo, since_date)
        df_repo = clean_pr_data(prs)
        df_repo['repo_full_name'] = repo
        all_data.append(df_repo)
    
    if all_data:
        df_final = pd.concat(all_data, ignore_index=True).drop_duplicates(subset=['id'])
        load_to_synapse(df_final, conn_str)  # Your load function (remove user/pass)
        logging.info(f'Loaded {len(df_final)} PRs successfully.')
    else:
        logging.warning('No new data to process.')
