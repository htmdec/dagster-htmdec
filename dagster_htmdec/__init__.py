from dagster._core.definitions.external_asset import (
    create_external_asset_from_source_asset,
)
from dasgter import (
    Definitions,
    EnvVar,
    FilesystemIOManager,
)

from .assets import pdv_sources, processed_pdv_data
from .jobs import pdv_job
from .partitions import pdv_partition
from .resources import GirderConnection, GirderCredentials
from .resources.io_manager import ConfigurableGirderIOManager
from .sensors import make_girder_folder_sensor

defs = Definitions(
    assets=[
        create_external_asset_from_source_asset(pdv_sources),
        processed_pdv_data,
    ],
    jobs=[pdv_job],
    sensors=[
        make_girder_folder_sensor(
            pdv_job,
            EnvVar("DATAFLOW_SRC_FOLDER_ID").get_value(),
            "pdv_watchdog",
            pdv_partition,
        )
    ],
    resources={
        "fs_io_manager": FilesystemIOManager(),
        "girder_io_manager": ConfigurableGirderIOManager(
            api_key=EnvVar("GIRDER_API_KEY"),
            api_url=EnvVar("GIRDER_API_URL"),
            source_folder_id=EnvVar("DATAFLOW_SRC_FOLDER_ID"),
            target_folder_id=EnvVar("DATAFLOW_DST_FOLDER_ID"),
        ),
        "girder": GirderConnection(
            credentials=GirderCredentials(
                api_key=EnvVar("GIRDER_API_KEY"), api_url=EnvVar("GIRDER_API_URL")
            )
        ),
    },
)
