import io
import os
from dagster import (
    IOManager,
    ConfigurableIOManagerFactory,
    OutputContext,
    InputContext,
    EnvVar,
)
from girder_client import GirderClient


class GirderIOManager(IOManager):
    def __init__(self, api_url, api_key):
        self._cli = GirderClient(apiUrl=api_url)
        self._cli.authenticate(apiKey=api_key)

    def _get_path(self, context) -> str:
        if context.has_partition_key:
            return "/".join(context.asset_key.path + [context.asset_partition_key])
        else:
            return "/".join(context.asset_key.path)

    def handle_output(self, context: OutputContext, obj):
        path = self._get_path(context)
        size = obj.seek(0, os.SEEK_END)
        obj.seek(0)
        self._cli.uploadStreamToFolder(
            "656fb6af6292cc8a7fc5ccf6",
            obj,
            os.path.basename(path),
            size=size,
        )

    def load_input(self, context: InputContext):
        path = self._get_path(context)
        children = list(
            self._cli.listItem("656fb6af6292cc8a7fc5ccf6", name=os.path.basename(path))
        )
        if len(children) != 1:
            raise Exception(
                f"Expected to find exactly one item at path {path}, but found {len(children)}"
            )

        files = self.cli.get("item/{}/files".format(children[0]["_id"]))
        if len(files) != 1:
            raise Exception(
                f"Expected to find exactly one file at path {path}, but found {len(files)}"
            )

        fp = io.BytesIO()
        self.cli.downloadFile(files[0]["_id"], fp)
        fp.seek(0)
        return fp


class ConfigurableGirderIOManager(ConfigurableIOManagerFactory):
    api_key: str = EnvVar("GIRDER_API_KEY")
    api_url: str = EnvVar("GIRDER_API_URL")

    def create_io_manager(self, context) -> GirderIOManager:
        return GirderIOManager(self.api_url, self.api_key)
