import io
import os
import urllib.parse as parse

from dagster import (
    ConfigurableIOManagerFactory,
    InputContext,
    IOManager,
    MetadataValue,
    OutputContext,
)
from girder_client import GirderClient


class GirderIOManager(IOManager):
    def __init__(self, api_url, token, source_folder_id, target_folder_id):
        self._cli = GirderClient(apiUrl=api_url)
        self._cli.token = token
        self.source_folder_id = source_folder_id
        self.target_folder_id = target_folder_id

    def _get_path(self, context) -> str:
        if context.has_partition_key:
            return "/".join(context.asset_key.path + [context.asset_partition_key])
        else:
            return "/".join(context.asset_key.path)

    def handle_output(self, context: OutputContext, obj):
        if not obj:
            return
        item, file = self._get_file(context, self.target_folder_id, suffix="png")
        size = obj.seek(0, os.SEEK_END)
        obj.seek(0)

        if file:
            fobj = self._cli.uploadFileContents(file["_id"], obj, size)
        else:
            file = self._cli.post(
                "file",
                parameters={
                    "parentType": "item",
                    "parentId": item["_id"],
                    "name": item["name"],
                    "size": size,
                    "mimeType": "image/png",
                },
            )
            fobj = self._cli._uploadContents(file, obj, size)
        girder_metadata = {
            "code_version": context.version,
            "run_id": context.run_id,
            "dataflow": os.environ.get("DATAFLOW_ID", "unknown"),
            "spec": os.environ.get("DATAFLOW_SPEC_ID", "unknown"),
        }
        self._cli.addMetadataToItem(item["_id"], girder_metadata)
        girder_url = parse.urlparse(self._cli.urlBase)
        metadata = {
            "size": fobj["size"],
            "item_url": MetadataValue.url(
                f"{girder_url.scheme}://{girder_url.netloc}/#item/{fobj['itemId']}"
            ),
            "download_url": MetadataValue.url(
                f"{self._cli.urlBase}file/{fobj['_id']}/download"
            ),
            "dataflow": os.environ.get("DATAFLOW_ID", "unknown"),
            "spec": os.environ.get("DATAFLOW_SPEC_ID", "unknown"),
            "docker_image": os.environ.get("DAGSTER_CURRENT_IMAGE", "unknown"),
        }
        if context.has_asset_key:
            context.add_output_metadata(metadata)

    def load_input(self, context: InputContext):
        item, file = self._get_file(context, self.source_folder_id)
        if not file:
            raise Exception(
                f"Expected to find exactly one file at path {item['name']}."
            )

        fp = io.BytesIO()
        self._cli.downloadFile(file["_id"], fp)
        fp.seek(0)
        return fp

    def _get_file(self, context, folder_id, suffix=None):
        path = self._get_path(context)
        name = ".".join(os.path.basename(path).rsplit("_", 1))
        if suffix:
            name = name.replace("csv", suffix)
        item = self._cli.loadOrCreateItem(name, folder_id)

        files = self._cli.get(
            f"item/{item['_id']}/files",
            parameters={"limit": 1, "offset": 0, "sort": "created", "sortdir": -1},
        )
        try:
            file = files[0]
        except (TypeError, IndexError):
            file = None
        return item, file


class ConfigurableGirderIOManager(ConfigurableIOManagerFactory):
    token: str
    api_url: str
    source_folder_id: str
    target_folder_id: str

    def create_io_manager(self, context) -> GirderIOManager:
        return GirderIOManager(
            self.api_url, self.token, self.source_folder_id, self.target_folder_id
        )
