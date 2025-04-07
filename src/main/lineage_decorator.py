import time
import uuid
import traceback
import functools
from datetime import datetime, timezone

# Assume client and namespace are accessible (e.g., from common_setup)
from openlineage_setup import client, OPENLINEAGE_NAMESPACE, producer_for_event
from db_utils2 import get_table_schema, get_table_row_count, get_column_lineage_facet

# Import OpenLineage models
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from openlineage.client.facet import (
    SourceCodeLocationJobFacet, NominalTimeRunFacet, ErrorMessageRunFacet,
    SqlJobFacet
)
#from openlineage.client.dataset import Dataset

import duckdb
from typing import List, Dict, Tuple, Optional

def log_lineage(job_name: str,
                input_tables: List[str] = [],
                output_tables: List[str] = [],
                sql: Optional[str] = None, # Pass the SQL string if available
                # Optionally pass manual column lineage map (Option B)
                column_lineage: Optional[Dict] = None,
                # Optionally pass script path if not discoverable
                script_path: Optional[str] = None):
    """
    Decorator to emit OpenLineage START and COMPLETE/FAIL events for a function
    executing an ETL job step (often involving SQL).
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Assumes the wrapped function receives a duckdb connection as an argument
            # Adjust if your connection handling is different
            db_conn = None
            for arg in args:
                if isinstance(arg, duckdb.DuckDBPyConnection):
                    db_conn = arg
                    break
            if db_conn is None:
                 # Or get connection via kwargs or global state if necessary
                raise ValueError("DuckDB connection not found in function arguments for lineage logging.")

            run_id = str(uuid.uuid4())
            start_time = datetime.now().isoformat()

            # --- Prepare START Event ---
            input_datasets_ol = []
            for table in input_tables:
                schema_facet = get_table_schema(db_conn, table)
                facets = {"schema": schema_facet} if schema_facet else {}
                input_datasets_ol.append(Dataset(
                    namespace=OPENLINEAGE_NAMESPACE, name=table, facets=facets
                ))

            # Try to get script location automatically
            source_location = script_path or func.__globals__.get('__file__')
            job_facets = {}
            if source_location:
                job_facets["sourceCodeLocation"] = SourceCodeLocationJobFacet(
                    type="python", # Or 'sql' if wrapping a script runner
                    url=source_location # file://... or git coordinates
                )
            # Add SQL Facet if SQL is provided
            if sql:
                job_facets["sql"] = SqlJobFacet(query=sql)

            client.emit(RunEvent(
                eventType=RunState.START,
                eventTime=start_time,
                run=Run(runId=run_id, facets={
                    "nominalTime": NominalTimeRunFacet(nominalStartTime=start_time)
                }),
                job=Job(namespace=OPENLINEAGE_NAMESPACE, name=job_name, facets=job_facets),
                producer=producer_for_event,
                inputs=input_datasets_ol,
                outputs=[] # Outputs typically defined at completion
            ))

            error = None
            result = None
            try:
                # --- Execute the actual function ---
                print(f"OL START: {job_name} (Run: {run_id})")
                result = func(*args, **kwargs)
                print(f"OL SUCCESS: {job_name} (Run: {run_id})")

            except Exception as e:
                print(f"OL FAIL: {job_name} (Run: {run_id}) - {e}")
                error = e
                trace = traceback.format_exc()
                # Capture error details for the FAIL event
                run_facets = {
                    "errorMessage": ErrorMessageRunFacet(
                        message=str(e),
                        programmingLanguage="python", # Or SQL if it's a SQL error
                        stackTrace=trace
                    )
                }
                client.emit(RunEvent(
                    eventType=RunState.FAIL,
                    eventTime=datetime.now().isoformat(),
                    run=Run(runId=run_id, facets=run_facets),
                    job=Job(namespace=OPENLINEAGE_NAMESPACE, name=job_name, facets=job_facets), # Include job facets
                    producer=producer_for_event,
                    inputs=input_datasets_ol, # Include inputs on fail
                    outputs=[]
                ))
                raise # Re-raise the exception

            finally:
                # --- Emit COMPLETE Event (if no error occurred) ---
                if error is None:
                    end_time = datetime.now().isoformat()
                    output_datasets_ol = []
                    for table in output_tables:
                        schema_facet = get_table_schema(db_conn, table)
                        stats_facet = get_table_row_count(db_conn, table)
                        # TODO: Implement column lineage facet generation
                        # Option A: Parse 'sql' variable here using sqlglot/sqllineage
                        # col_lineage_facet = parse_sql_for_lineage(sql, input_tables, output_tables)
                        # Option B: Use manually passed map
                        col_lineage_facet = get_column_lineage_facet(column_lineage)

                        facets = {}
                        if schema_facet: facets["schema"] = schema_facet
                        if stats_facet: facets["outputStatistics"] = stats_facet
                        if col_lineage_facet: facets["columnLineage"] = col_lineage_facet # Add column lineage

                        output_datasets_ol.append(Dataset(
                            namespace=OPENLINEAGE_NAMESPACE, name=table, facets=facets
                        ))

                    # Add end time to run facets
                    run_facets = {
                        "nominalTime": NominalTimeRunFacet(
                            nominalStartTime=start_time,
                            nominalEndTime=end_time
                        )
                    }

                    client.emit(RunEvent(
                        eventType=RunState.COMPLETE,
                        eventTime=end_time,
                        run=Run(runId=run_id, facets=run_facets),
                        job=Job(namespace=OPENLINEAGE_NAMESPACE, name=job_name, facets=job_facets), # Include job facets
                        producer=producer_for_event,
                        inputs=input_datasets_ol, # Optional on complete, but good practice
                        outputs=output_datasets_ol
                    ))
            return result # Return the original function's result
        return wrapper
    return decorator