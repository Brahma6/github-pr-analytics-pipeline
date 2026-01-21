# GitHub PR Analytics Pipeline

A Python-based Azure Functions application that fetches, cleans, and loads GitHub pull request data to Azure Synapse Analytics for analytics and reporting.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Environment Setup](#environment-setup)
- [Local Development](#local-development)
- [Running in Azure](#running-in-azure)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

## Prerequisites

Before you begin, ensure you have the following installed:

### Required Software
- **Python 3.9+** ([Download](https://www.python.org/downloads/))
- **Azure Functions Core Tools** ([Installation Guide](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local))
- **Git** ([Download](https://git-scm.com/))
- **pyodbc prerequisites**:
  - **Windows**: [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
  - **macOS/Linux**: Install via Homebrew or package manager

### Required Accounts & Credentials
- **GitHub**: A GitHub Personal Access Token (PAT) with `repo` and `read:repo_hook` scopes
  - [Create GitHub PAT](https://github.com/settings/tokens)
- **Azure Synapse**: Connection details (server, database, username, password)

## Project Structure

```
github-pr-analytics-pipeline/
├── function_app.py              # Azure Functions timer trigger entry point
├── github_pr_pipeline_Local.py   # Standalone local execution script
├── fetch_clean_load.py           # Core ETL functions (fetch, clean, load)
├── synapse_connector.py          # Synapse database connection utilities
├── local.settings.json           # Local development configuration (not in git)
├── requirements.txt              # Python dependencies
├── host.json                     # Azure Functions host configuration
├── README.md                     # This file
└── chicago_taxi_trip_analytics/  # SQL query examples
    ├── sessions_count_taxi.sql
    └── top_trip_earners.sql
```

## Environment Setup

### Step 1: Clone or Download the Repository

```bash
git clone <repository-url>
cd github-pr-analytics-pipeline
```

### Step 2: Create a Python Virtual Environment

**Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**macOS/Linux:**
```bash
python -m venv venv
source venv/bin/activate
```

### Step 3: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Create & Configure `local.settings.json`

Create a `local.settings.json` file in the project root with your credentials:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "GITHUB_TOKEN": "github_pat_YOUR_TOKEN_HERE",
    "SYNAPSE_SERVER": "tcp:your-server.sql.azuresynapse.net",
    "SYNAPSE_DB": "your_database_name",
    "SYNAPSE_USER": "sqladminuser",
    "SYNAPSE_PASS": "your_password_here",
    "TABLE_NAME": "github_pull_requests",
    "DAYS_BACK": "30"
  },
  "ConnectionStrings": {}
}
```

**Important**: Add `local.settings.json` to `.gitignore` to prevent exposing credentials!

### Step 5: Generate GitHub Personal Access Token

1. Go to [GitHub Settings > Tokens](https://github.com/settings/tokens)
2. Click "Generate new token (classic)" or "Generate new token (fine-grained)"
3. Grant scopes: `repo` and `read:repo_hook`
4. Copy the token and add it to `local.settings.json` as `GITHUB_TOKEN`

## Local Development

### Running the Local Script

To fetch and process PR data locally:

```bash
python github_pr_pipeline_Local.py
```

This script:
- Fetches PRs from GitHub repos
- Cleans and transforms the data
- connect to synapse database and write the data

### Running the Azure Functions Locally

To test the Azure Functions runtime:

```bash
func start
```

This will:
- Start the Azure Functions emulator
- Run the timer trigger (configured to run every minute in development)
- Show logs in the terminal

**Access the function:**
- The function runs on a timer trigger (no HTTP endpoint by default)
- Logs will display when the trigger fires

## Running in Azure

### Prerequisites
- Azure subscription
- Azure Functions app created in Azure Portal
- Azure Synapse Analytics instance

### Deployment Steps

1. **Install Azure CLI**:
   ```bash
   az login
   ```

2. **Deploy to Azure Functions**:
   ```bash
   func azure functionapp publish <YOUR_FUNCTION_APP_NAME>
   ```

3. **Configure Application Settings in Azure Portal**:
   - Go to Azure Portal → Function App → Settings → Environment Variables
   - Add all values from `local.settings.json` (except `IsEncrypted` and `FUNCTIONS_WORKER_RUNTIME`)
   - For production, use Azure Key Vault instead of plain text credentials

4. **Configure Managed Identity** (Recommended for Security):
   - Enable Managed Identity on your Function App
   - Assign it SQL Server roles in Synapse
   - Remove `SYNAPSE_USER` and `SYNAPSE_PASS` from Application Settings
   - The code will use Managed Identity for authentication

## Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `GITHUB_TOKEN`   | GitHub Personal Access Token | `github_pat_11AC...` |
| `SYNAPSE_SERVER` | Azure Synapse server address | `tcp:gitpranalytics.sql.azuresynapse.net` |
| `SYNAPSE_DB` | Database name | `githubprpool` |
| `SYNAPSE_USER` | SQL username (local dev only) | `sqladminpr` |
| `SYNAPSE_PASS` | SQL password (local dev only) | `Password123!` |
| `TABLE_NAME` | Target table in Synapse | `github_pull_requests` |
| `DAYS_BACK` | Days of PR history to fetch | `30` |
| `REPOS` | Comma-separated repos to track | `Brahma6/testing_repo,owner/repo2` |

### Timer Trigger Schedule

The timer trigger uses CRON expression format:
```csharp
@app.timer_trigger(schedule="0 0 9 * * *")  # Daily at 9 AM UTC
```

## Troubleshooting

### Error 401: Bad credentials (GitHub)

**Cause**: Invalid or expired GitHub PAT

**Solution**:
1. Regenerate a new GitHub PAT: https://github.com/settings/tokens
2. Update `GITHUB_TOKEN` in `local.settings.json`
3. Restart the function

### Error: SYNAPSE_SERVER or SYNAPSE_DB not configured

**Cause**: Environment variables missing in `local.settings.json`

**Solution**:
1. Ensure all Synapse variables are in `local.settings.json`
2. Verify `IsEncrypted: false` in the file
3. Restart `func start`

### Error: ODBC Driver 18 not found

**Cause**: SQL Server ODBC driver not installed

**Solution**:
- **Windows**: Install [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- **macOS**: `brew install unixodbc`
- **Linux**: `sudo apt-get install unixodbc unixodbc-dev`

### `local.settings.json` keeps resetting to defaults

**Cause**: File marked as encrypted but decryption key unavailable

**Solution**:
1. Delete `local.settings.json`
2. Create a new one with `"IsEncrypted": false` (see Step 4 above)
3. Never commit this file to git

### Pip install fails for pyodbc

**Cause**: Missing C++ build tools or ODBC headers

**Solution**:
- **Windows**: Install [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
- **macOS/Linux**: Install development headers (see Prerequisites)

## Additional Resources

- [Azure Functions Python Developer Guide](https://learn.microsoft.com/en-us/azure/azure-functions/functions-reference-python)
- [GitHub REST API Documentation](https://docs.github.com/en/rest)
- [Azure Synapse Analytics Documentation](https://learn.microsoft.com/en-us/azure/synapse-analytics/)
- [Azure Functions Timer Trigger](https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-timer)

## Security Best Practices

- ✅ Never commit `local.settings.json` to version control
- ✅ Use Azure Key Vault for secrets in production
- ✅ Enable Managed Identity for Azure resources
- ✅ Rotate GitHub PAT and database passwords regularly
- ✅ Use least-privilege access for database users
- ✅ Monitor function execution logs in Azure Monitor

