from synapse_connector import SynapseConnector

connector = SynapseConnector("gitpranalytics.sql.azuresynapse.net", "githubprpool", "sqladminpr", "Nikky@3012")
if connector.connect():
    results = connector.execute_query("SELECT * FROM github_pull_requests")
    print(f"Fetched {len(results)} rows from github_pull_requests table.")
    connector.disconnect()