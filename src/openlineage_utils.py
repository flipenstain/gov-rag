from openlineage.client.event_v2 import (
    Dataset,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import (
    nominal_time_run,
    schema_dataset,
    source_code_location_job,
    sql_job,
    column_lineage_dataset,
)
import os
from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client import OpenLineageClient
from typing import Any, Dict, List, Optional
from openlineage.client.uuid import generate_new_uuid
from datetime import datetime


# Ensure the logging directory exists
log_dir = "src/logging"
os.makedirs(log_dir, exist_ok=True)

file_config = FileConfig(
    log_file_path="src/logging/ol_log",
    append=False,
)

client = OpenLineageClient(transport=FileTransport(file_config))

## VAJA MUUTA ##

PRODUCER = "https://github.com/openlineage-user"
namespace = "python_client"
#dag_name = "user_trends" 

# generates job facet
def job(job_name, sql, location):
    facets = {"sql": sql_job.SQLJobFacet(query=sql)}
    if location != None:
        facets.update(
            {
                "sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(
                    "git", location
                )
            }
        )
    return Job(namespace=namespace, name=job_name, facets=facets)


# generates run facet
def run(hour):
    return Run(
        runId=str(generate_new_uuid()),
        facets={
            "nominalTime": nominal_time_run.NominalTimeRunFacet(
                nominalStartTime=f"2022-04-14T{twoDigits(hour)}:12:00Z",
                # nominalEndTime=None
            )
        },
    )


# generates dataset
def dataset(name: str, schema: Optional[any] = None, ns: str = "default_namespace") -> Dataset:
    if schema is None:
        facets = {}
    else:
        facets = {"schema": schema}
    return Dataset(namespace=ns, name=name, facets=facets)

def create_schema_facet(columns: List[Dict[str, str]]) -> schema_dataset.SchemaDatasetFacet:
    """Creates a SchemaDatasetFacet from a list of column definitions."""
    fields = [
        schema_dataset.SchemaDatasetFacetFields(name=col["name"], type=col["type"], description=col["description"]) for col in columns
    ]
    return schema_dataset.SchemaDatasetFacet(fields=fields)

# generates output dataset
def outputDataset(dataset, stats, column_lineage = None):
    output_facets = {"outputStatistics": stats, "columnLineage": column_lineage}
    return OutputDataset(dataset.namespace,
                         dataset.name,
                         facets=dataset.facets,
                         outputFacets=output_facets)

def create_column_lineage(
    input_fields: List[column_lineage_dataset.InputField],
) -> column_lineage_dataset.ColumnLineageDatasetFacet:
    """Creates a ColumnLineage object."""
    return column_lineage_dataset.ColumnLineageDatasetFacet(inputFields=input_fields)

##

# Utility Functions
def create_transformation(type: str, subtype: Optional[str] = None, description: Optional[str] = None, masking: Optional[bool] = None) -> column_lineage_dataset.Transformation:
    """Creates a Transformation object."""
    return column_lineage_dataset.Transformation(type=type, subtype=subtype, description=description, masking=masking)

def create_input_field(namespace: str, name: str, field: str, transformations: Optional[List[column_lineage_dataset.Transformation]] = None) -> column_lineage_dataset.InputField:
    """Creates an InputField object."""
    return column_lineage_dataset.InputField(namespace=namespace, name=name, field=field, transformations=transformations)

def create_fields(input_fields: List[column_lineage_dataset.InputField], transformation_description: Optional[str] = None, transformation_type: Optional[str] = None) -> column_lineage_dataset.Fields:
    """Creates a Fields object."""
    return column_lineage_dataset.Fields(inputFields=input_fields, transformationDescription=transformation_description, transformationType=transformation_type)

def create_column_lineage_dataset_facet(fields: Dict[str, column_lineage_dataset.Fields], dataset: Optional[List[column_lineage_dataset.InputField]] = None) -> column_lineage_dataset.ColumnLineageDatasetFacet:
    """Creates a ColumnLineageDatasetFacet object."""
    return column_lineage_dataset.ColumnLineageDatasetFacet(fields=fields, dataset=dataset)

##


# generates input dataset
def inputDataset(dataset, dq):
    input_facets = {
        "dataQuality": dq,
    }
    return InputDataset(dataset.namespace, dataset.name,
                        facets=dataset.facets,
                        inputFacets=input_facets)


def twoDigits(n):
    if n < 10:
        result = f"0{n}"
    elif n < 100:
        result = f"{n}"
    else:
        raise f"error: {n}"
    return result


now = datetime.now()


# generates run Event
def runEvents(job_name, sql, inputs, outputs, hour, min, location, duration):
    run_id = str(generate_new_uuid())
    myjob = job(job_name, sql, location)
    myrun = run(hour)
    started_at = datetime.now()
    ended_at = datetime.now()
    return (
        RunEvent(
            eventType=RunState.START,
            eventTime=started_at.isoformat(),
            run=myrun,
            job=myjob,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ),
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now() + duration,
            run=myrun,
            job=myjob,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ),
    )


# add run event to the events list
def addRunEvents(events, job_name, sql, inputs, outputs, hour, minutes, location=None, duration=2):
    (start, complete) = runEvents(job_name, sql, inputs, outputs, hour, minutes, location, duration)
    events.append(start)
    events.append(complete)

""" 
events = []

# create dataset data
for i in range(0, 5):
    user_counts = dataset("tmp_demo.user_counts")
    user_history = dataset(
        "temp_demo.user_history",
        schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(
                    name="id", type="BIGINT", description="the user id"
            ]
        ),
        "snowflake://",
    )

    create_user_counts_sql = 3xCREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (
            SELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count
            FROM TMP_DEMO.USER_HISTORY
            GROUP BY date
            ) 3x

    # location of the source code
    location = "https://github.com/some/airflow/dags/example/user_trends.py"

    # run simulating Airflow DAG with snowflake operator
    addRunEvents(
        events,
        dag_name + ".create_user_counts",
        create_user_counts_sql,
        [user_history], # inputs
        [user_counts], # outputs
        i,
        11,
        location,
    )


for event in events:
    from openlineage.client.serde import Serde

    print(event)
    print(Serde.to_json(event))
    # time.sleep(1)
    client.emit(event)"
    ""
"""


if __name__ == "__main__":
    print("This runs only when lineage_utils.py is executed directly")


# Test Input
schema_columns = [
    {"name": "user_id", "type": "INT", "description": "User ID"},
    {"name": "product_name", "type": "STRING", "description": "Product Name"}
]

schema_facet = create_schema_facet(schema_columns)

my_dataset = dataset(name="my_output_data", schema=schema_facet, ns="my_namespace")

stats_data = {"rowCount": 1000, "fileSize": "1MB"}

transformations = [create_transformation(type="FILTER", description="Filter out invalid users")]

input_fields = [create_input_field(namespace="input_ns", name="input_dataset", field="user_id", transformations=transformations)]

fields_data = {"user_id": create_fields(input_fields=input_fields, transformation_type="FILTER")}

column_lineage_facet = create_column_lineage_dataset_facet(fields=fields_data)

output_dataset_result = outputDataset(dataset=my_dataset, stats=stats_data, column_lineage=column_lineage_facet)

# Print the result to verify
print(output_dataset_result)
test_job_name = "LOADING_DATA"
test_sql = """SELECT * FROM WORLD"""
test_location = "git_hub_kodu"
test_job = job(test_job_name, test_sql, test_location)
print(test_job)

