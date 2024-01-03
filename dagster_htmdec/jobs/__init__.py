import os

from dagster import define_asset_job
from dagster_docker import docker_executor

executor = docker_executor.configured(
    {
        "env_vars": [
            f"GIRDER_TOKEN={os.environ['GIRDER_TOKEN']}",
            f"GIRDER_API_URL={os.environ['GIRDER_API_URL']}",
            f"DATAFLOW_ID={os.environ['DATAFLOW_ID']}",
            f"DATAFLOW_SPEC_ID={os.environ['DATAFLOW_SPEC_ID']}",
            f"DATAFLOW_SRC_FOLDER_ID={os.environ['DATAFLOW_SRC_FOLDER_ID']}",
            f"DATAFLOW_DST_FOLDER_ID={os.environ['DATAFLOW_DST_FOLDER_ID']}",
        ],
        "container_kwargs": {
            "extra_hosts": {"girder.local.wholetale.org": "host-gateway"}
        },
    }
)

demo_job = define_asset_job(
    name="demo_processing_job",
    selection="processed_demo_data",
    executor_def=executor,
)
