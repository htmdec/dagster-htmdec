import io

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    IdentityPartitionMapping,
    SourceAsset,
    asset,
)

from ..partitions import pdv_partition

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
