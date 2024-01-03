from dagster._core.definitions.external_asset import (
    create_external_asset_from_source_asset,
)
from dagster import (
    Definitions,
    EnvVar,
    FilesystemIOManager,
)

from .assets import demo_sources, processed_demo_data
from .jobs import demo_job
from .partitions import demo_partition
from .resources import GirderConnection, GirderCredentials
from .resources.io_manager import ConfigurableGirderIOManager
from .sensors import make_girder_folder_sensor

defs = Definitions(
    assets=[
        create_external_asset_from_source_asset(demo_sources),
        processed_demo_data,
    ],
    jobs=[demo_job],
    sensors=[
        make_girder_folder_sensor(
            demo_job,
            EnvVar("DATAFLOW_SRC_FOLDER_ID").get_value(),
            "demo_watchdog",
            demo_partition,
        )
    ],
    resources={
        "fs_io_manager": FilesystemIOManager(),
        "girder_io_manager": ConfigurableGirderIOManager(
            token=EnvVar("GIRDER_TOKEN"),
            api_url=EnvVar("GIRDER_API_URL"),
            source_folder_id=EnvVar("DATAFLOW_SRC_FOLDER_ID"),
            target_folder_id=EnvVar("DATAFLOW_DST_FOLDER_ID"),
        ),
        "girder": GirderConnection(
            credentials=GirderCredentials(
                token=EnvVar("GIRDER_TOKEN"), api_url=EnvVar("GIRDER_API_URL")
            )
        ),
    },
)
