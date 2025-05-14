import os
from openlineage.client import OpenLineageClient
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    Dataset,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState
)
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

producer = "OpenLineage.io/website/blog"

inventory = Dataset(namespace="food_delivery", name="public.inventory")
menus = Dataset(namespace="food_delivery", name="public.menus_1")
orders = Dataset(namespace="food_delivery", name="public.orders_1")

job = Job(namespace="food_delivery", name="example.order_data")
run = Run(runId=str(generate_new_uuid()))

client.emit(
    RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now().isoformat(),
        run=run,
        job=job,
        producer=producer,
    )
)

client.emit(
    RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now().isoformat(),
        run=run, job=job, producer=producer,
        inputs=[inventory],
        outputs=[menus, orders],
    )
)


