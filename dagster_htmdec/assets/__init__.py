import io

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetIn,
    IdentityPartitionMapping,
    Output,
    SourceAsset,
    asset,
)

from ..partitions import demo_partition

demo_sources = SourceAsset(
    "demo_sources",
    io_manager_key="girder_io_manager",
    partitions_def=demo_partition,
)


@asset(
    ins={"demo_sources": AssetIn(partition_mapping=IdentityPartitionMapping())},
    io_manager_key="girder_io_manager",
    partitions_def=demo_partition,
    check_specs=[AssetCheckSpec(name="negative_slope", asset="processed_demo_data")],
)
def processed_demo_data(
    context: AssetExecutionContext, demo_sources: io.BytesIO
) -> io.BytesIO:
    df = pd.read_csv(demo_sources)
    b, a = np.polyfit(df["x"], df["y"], deg=1)

    plt.figure()
    plt.scatter(df["x"], df["y"])
    xseq = np.linspace(min(df["x"]), max(df["x"]), 100)
    plt.plot(xseq, a + b * xseq, color="k", lw=2.5)
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    yield Output(value=buf)

    # perform check
    yield AssetCheckResult(
        passed=bool(b > 0),
        metadata={"slope": b, "intercept": a, "equation": f"y = {a} + {b}x", "run_id": context.run_id}
    )
