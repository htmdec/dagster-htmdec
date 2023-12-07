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
    def __init__(self, api_url, api_key, source_folder_id, target_folder_id):
        self._cli = GirderClient(apiUrl=api_url)
        self._cli.authenticate(apiKey=api_key)
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
        path = self._get_path(context)
        # TODO make it generic
        name = ".".join(os.path.basename(path).rsplit("_", 1)).replace("csv", "png")
        size = obj.seek(0, os.SEEK_END)
        obj.seek(0)

        fobj = self._cli.uploadStreamToFolder(
            self.target_folder_id,
            obj,
            name,
            size=size,
            mimeType="image/png",
        )
        girder_metadata = {
            "code_version": context.version,
            "run_id": context.run_id,
            "dataflow": os.environ.get("DATAFLOW_ID", "unknown"),
            "spec": os.environ.get("DATAFLOW_SPEC_ID", "unknown"),
        }
        self._cli.addMetadataToItem(fobj["itemId"], girder_metadata)
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
        print("In input")
        path = self._get_path(context)
        name = ".".join(os.path.basename(path).rsplit("_", 1))
        children = list(self._cli.listItem(self.source_folder_id, name=name))
        if len(children) != 1:
            raise Exception(
                f"Expected to find exactly one item at path {path}, but found {len(children)}"
            )

        files = self._cli.get("item/{}/files".format(children[0]["_id"]))
        if len(files) != 1:
            raise Exception(
                f"Expected to find exactly one file at path {path}, but found {len(files)}"
            )

        fp = io.BytesIO()
        self._cli.downloadFile(files[0]["_id"], fp)
        fp.seek(0)
        return fp


class ConfigurableGirderIOManager(ConfigurableIOManagerFactory):
    api_key: str
    api_url: str
    source_folder_id: str
    target_folder_id: str

    def create_io_manager(self, context) -> GirderIOManager:
        return GirderIOManager(
            self.api_url, self.api_key, self.source_folder_id, self.target_folder_id
        )
