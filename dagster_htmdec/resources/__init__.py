import io
from contextlib import contextmanager

import girder_client
from dagster import ConfigurableResource
from pydantic import PrivateAttr


class GirderCredentials(ConfigurableResource):
    api_url: str
    token: str


class GirderConnection(ConfigurableResource):
    credentials: GirderCredentials
    _client: girder_client.GirderClient = PrivateAttr()

    @contextmanager
    def yield_for_execution(self, context):
        self._client = girder_client.GirderClient(apiUrl=self.credentials.api_url)
        self._client.token = self.credentials.token
        yield self

    def list_folder(self, folder_id):
        return list(self._client.listFolder(folder_id))

    def list_item(self, folder_id):
        return list(self._client.listItem(folder_id))

    def get_item(self, item_id):
        return self._client.getItem(item_id)

    def get_file_from_item(self, item_id):
        files = self._client.get(
            f"item/{item_id}/files",
            parameters={"limit": 1, "offset": 0, "sort": "created", "sortdir": -1},
        )
        return files[0]

    def get_stream(self, item_id):
        fobj = self.get_file_from_item(item_id)
        data = io.BytesIO()
        self._client.downloadFile(fobj["_id"], data)
        data.seek(0)
        return data
