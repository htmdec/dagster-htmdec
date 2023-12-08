import json
import os
import time

import numpy as np
import requests
from girder_client import GirderClient

UPLOAD = True
rng = np.random.RandomState()
if UPLOAD:
    gc = GirderClient(apiUrl=os.environ.get("GIRDER_API_URL"))
    gc.token = os.environ.get("GIRDER_TOKEN")
    gc.delete(f"/folder/{os.environ.get('DATAFLOW_SRC_FOLDER_ID')}/contents")


def create_external_asset_materialization(data):
    url = "http://localhost:3000/report_asset_materialization/"
    headers = {"content-type": "application/json"}
    response = requests.post(
        url=url,
        data=json.dumps(data),
        headers=headers,
    )

    if response.status_code != 200:
        print(  # noqa: T201
            f"Failed to create materialization. Status Code: {response.status_code}, Message:"
            f" {response.text}"
        )


def generate_data(filename):
    x = rng.uniform(0, 10, size=100)
    y = rng.normal(size=100)
    np.savetxt(filename, np.vstack((x, y)).T, delimiter=",", header="x,y", comments="")


counter = 0
while True:
    name = f"plot--{counter:05d}.csv"
    generate_data(name)
    if UPLOAD:
        fobj = gc.uploadFileToFolder(
            os.environ.get("DATAFLOW_SRC_FOLDER_ID"),
            name,
            mimeType="text/csv",
        )
        item_uri = os.path.join(gc.urlBase, "#item", str(fobj["itemId"]))
        payload = {
            "uri": item_uri,
            "asset_key": "pdv_sources",
            "partition": name,
            "data_version": "60bc881",
            "description": "Random scatter plot",
            "metadata": {
                "uri": item_uri,
            },
        }

        create_external_asset_materialization(payload)
        print(f"Created materialization for {name}")  # noqa: T201
        counter += 1
        time.sleep(10)
