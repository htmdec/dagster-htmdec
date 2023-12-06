import io

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    Definitions,
    DynamicPartitionsDefinition,
    EnvVar,
    FilesystemIOManager,
    IdentityPartitionMapping,
    SourceAsset,
    asset,
    define_asset_job,
)
from dagster._core.definitions.external_asset import (
    create_external_asset_from_source_asset,
)

from .io_manager import ConfigurableGirderIOManager
from .resources import GirderConnection, GirderCredentials
from .sensors import make_girder_folder_sensor

pdv_partition = DynamicPartitionsDefinition(name="pdv_items")

pdv_sources = SourceAsset(
    "pdv_sources",
    io_manager_key="girder_io_manager",
    partitions_def=pdv_partition,
)


@asset(
    ins={"pdv_sources": AssetIn(partition_mapping=IdentityPartitionMapping())},
    io_manager_key="girder_io_manager",
    partitions_def=pdv_partition,
)
def processed_pdv_data(
    context: AssetExecutionContext, pdv_sources: io.BytesIO
) -> io.BytesIO:
    df = pd.read_csv(pdv_sources)
    b, a = np.polyfit(df["x"], df["y"], deg=1)

    plt.figure()
    plt.scatter(df["x"], df["y"])
    xseq = np.linspace(min(df["x"]), max(df["x"]), 100)
    plt.plot(xseq, a + b * xseq, color="k", lw=2.5)
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    return buf


pdv_job = define_asset_job(name="pdv_processing_job", selection="processed_pdv_data")

defs = Definitions(
    assets=[
        create_external_asset_from_source_asset(pdv_sources),
        processed_pdv_data,
    ],
    jobs=[pdv_job],
    sensors=[
        make_girder_folder_sensor(
            pdv_job,
            EnvVar("GIRDER_SRC_FOLDER_ID").get_value(),
            "pdv_watchdog",
            pdv_partition,
        )
    ],
    resources={
        "fs_io_manager": FilesystemIOManager(),
        "girder_io_manager": ConfigurableGirderIOManager(
            api_key=EnvVar("GIRDER_API_KEY"),
            api_url=EnvVar("GIRDER_API_URL"),
            source_folder_id=EnvVar("GIRDER_SRC_FOLDER_ID"),
            target_folder_id=EnvVar("GIRDER_DST_FOLDER_ID"),
        ),
        "girder": GirderConnection(
            credentials=GirderCredentials(
                api_key=EnvVar("GIRDER_API_KEY"), api_url=EnvVar("GIRDER_API_URL")
            )
        ),
    },
)
