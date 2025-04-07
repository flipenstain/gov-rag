import functools
import uuid
import traceback
from datetime import datetime, timezone
import duckdb # Make sure duckdb is imported

# Assume client, namespace, utils like get_table_schema etc. are imported
from common_setup import client, OPENLINEAGE_NAMESPACE
from db_utils import get_table_schema, get_table_row_count, get_column_lineage_facet
from openlineage.client.run import RunEvent, RunState, Run, Job
from openlineage.client.facet import (SourceCodeLocationJobFacet, NominalTimeRunFacet,
                                      ErrorMessageRunFacet, SqlJobFacet)
from openlineage.client.dataset import Dataset

# Decorator takes no OL arguments directly
def log_lineage_dynamic():
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # --- Find DB connection and Metadata from args/kwargs ---
            db_conn = None
            job_metadata = None

            # Prioritize finding metadata in kwargs first
            if 'job_metadata' in kwargs and isinstance(kwargs['job_metadata'], dict):
                job_metadata = kwargs['job_metadata']
            # If not in kwargs, search args for a dictionary likely being metadata
            else:
                for arg in args:
                    if isinstance(arg, dict) and 'job_name' in arg: # Heuristic check
                        job_metadata = arg
                        break

            if not job_metadata:
                raise ValueError("Dynamic lineage decorator requires 'job_metadata' dictionary argument.")

            # Find the DB connection (assuming it's passed explicitly)
            if 'db_conn' in kwargs and isinstance(kwargs['db_conn'], duckdb.DuckDBPyConnection):
                 db_conn = kwargs['db_conn']
            else:
                 for arg in args:
                     if isinstance(arg, duckdb.DuckDBPyConnection):
                         db_conn = arg
                         break

            if not db_conn:
                raise ValueError("Dynamic lineage decorator requires 'db_conn' DuckDBPyConnection argument.")


            # --- Extract metadata from the found dictionary ---
            job_name = job_metadata.get('job_name', f'unknown_job_{func.__name__}')
            input_tables = job_metadata.get('input_tables', [])
            output_tables = job_metadata.get('output_tables', [])
            sql = job_metadata.get('sql')
            column_lineage = job_metadata.get('column_lineage')
            # Get script path from metadata if provided, otherwise guess from function
            script_path = job_metadata.get('script_path', func.__globals__.get('__file__'))

            # --- Remainder is similar to the original decorator, ---
            # --- but uses variables extracted above ---
            run_id = str(uuid.uuid4())
            start_time = datetime.now(timezone.utc)

            # --- Prepare START Event ---
            input_datasets_ol = []
            for table in input_tables:
                schema_facet = get_table_schema(db_conn, table)
                facets = {"schema": schema_facet} if schema_facet else {}
                input_datasets_ol.append(Dataset(namespace=OPENLINEAGE_NAMESPACE, name=table, facets=facets))

            job_facets = {}
            if script_path:
                job_facets["sourceCodeLocation"] = SourceCodeLocationJobFacet(type="python", url=script_path)
            if sql:
                job_facets["sql"] = SqlJobFacet(query=sql)

            client.emit(RunEvent(
                eventType=RunState.START,
                eventTime=start_time,
                run=Run(runId=run_id, facets={"nominalTime": NominalTimeRunFacet(nominalStartTime=start_time)}),
                job=Job(namespace=OPENLINEAGE_NAMESPACE, name=job_name, facets=job_facets),
                inputs=input_datasets_ol, outputs=[]
            ))
            print(f"OL START (Dyn): {job_name} (Run: {run_id})")

            error = None
            result = None
            try:
                # Execute the actual function (passing all original args/kwargs)
                result = func(*args, **kwargs)
                print(f"OL SUCCESS (Dyn): {job_name} (Run: {run_id})")
            except Exception as e:
                error = e
                trace = traceback.format_exc()
                run_facets = { "errorMessage": ErrorMessageRunFacet(message=str(e), programmingLanguage="python", stackTrace=trace)}
                client.emit(RunEvent(eventType=RunState.FAIL, eventTime=datetime.now(timezone.utc),
                    run=Run(runId=run_id, facets=run_facets), job=Job(namespace=OPENLINEAGE_NAMESPACE, name=job_name, facets=job_facets),
                    inputs=input_datasets_ol, outputs=[] ))
                print(f"OL FAIL (Dyn): {job_name} (Run: {run_id}) - {e}")
                raise
            finally:
                if error is None:
                    # --- Emit COMPLETE Event ---
                    end_time = datetime.now(timezone.utc)
                    output_datasets_ol = []
                    for table in output_tables:
                        # Gather output facets using extracted metadata
                        schema_facet = get_table_schema(db_conn, table)
                        stats_facet = get_table_row_count(db_conn, table)
                        col_lineage_facet = get_column_lineage_facet(column_lineage) # Use extracted map
                        facets = {}
                        if schema_facet: facets["schema"] = schema_facet
                        if stats_facet: facets["outputStatistics"] = stats_facet
                        if col_lineage_facet: facets["columnLineage"] = col_lineage_facet
                        output_datasets_ol.append(Dataset(namespace=OPENLINEAGE_NAMESPACE, name=table, facets=facets))

                    run_facets = {"nominalTime": NominalTimeRunFacet(nominalStartTime=start_time, nominalEndTime=end_time)}
                    client.emit(RunEvent(eventType=RunState.COMPLETE, eventTime=end_time,
                        run=Run(runId=run_id, facets=run_facets), job=Job(namespace=OPENLINEAGE_NAMESPACE, name=job_name, facets=job_facets),
                        inputs=input_datasets_ol, outputs=output_datasets_ol ))
                    print(f"OL COMPLETE (Dyn): {job_name} (Run: {run_id})")
            return result
        return wrapper
    return decorator