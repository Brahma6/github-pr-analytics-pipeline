import logging
import azure.functions as func
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
import logging
from time import sleep
import fetch_clean_load as fcl # Import fetch, clean and Load functions

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config from App Settings
DAYS_BACK = int(os.getenv('DAYS_BACK', 30))
REPOS = os.getenv('REPOS', 'Brahma6/testing_repo').split(',')    # For testing
#REPOS = ['microsoft/TypeScript']  # For testing

app = func.FunctionApp()
@app.timer_trigger(schedule="0 9 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)  # Daily at 9AM UTC

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
    since_date = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).isoformat() + 'Z'
    all_data = []
    
    for repo in REPOS:
        prs = fcl.fetch_prs(repo, since_date)
        df_repo = fcl.clean_pr_data(prs)
        df_repo['repo_full_name'] = repo
        all_data.append(df_repo)
    
    if all_data:
        df_final = pd.concat(all_data, ignore_index=True).drop_duplicates(subset=['id'])
        fcl.load_to_synapse(df_final, conn_str)  # Your load function (remove user/pass)
        logging.info(f'Loaded {len(df_final)} PRs successfully.')
    else:
        logging.warning('No new data to process.')
