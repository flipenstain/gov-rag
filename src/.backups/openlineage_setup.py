import os
from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport

def get_openlineage_client():
    """Initializes and returns the OpenLineage client."""
    # Ensure the logging directory exists
    log_dir = "src/logging"
    os.makedirs(log_dir, exist_ok=True)

    file_config = FileConfig(
        log_file_path="src/logging/ol_log",
        append=False,
    )

    return OpenLineageClient(transport=FileTransport(file_config))

client = get_openlineage_client()
# OPENLINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "my_duckdb_etl")
OPENLINEAGE_NAMESPACE = "TPC-DI: duckdb-etl"
producer_for_event = "HARDCODED FOR NOW"