from typing import Any
from urllib.parse import urljoin

import requests
from fastapi import HTTPException, UploadFile

from odp.api.lib.archive import ArchiveAdapter, FileInfo
from odp.config import config


class FilestoreArchiveAdapter(ArchiveAdapter):
    """Adapter for the ODP file storage service, providing read-write
    access to Nextcloud or other filesystem-based archives.

    Integrates with `ODP Filing <https://github.com/SAEON/odp-filing>`_,
    which must be running on the server.
    """

    def __init__(self, download_url: str | None, upload_url: str | None) -> None:
        super().__init__(download_url, upload_url)
        self.timeout = 3600.0 if config.ODP.ENV == 'development' else 10.0

    async def put(
            self,
            folder: str,
            filename: str,
            file: UploadFile,
            sha256: str,
            unpack: bool,
    ) -> list[FileInfo]:
        await file.seek(0)
        params = {'filename': filename, 'sha256': sha256}
        if unpack:
            params |= {'unpack': 1}

        result = self._send_request(
            'PUT',
            urljoin(self.upload_url, folder),
            files={'file': file.file},
            params=params,
        )
        return [
            FileInfo(path, info[0], info[1])
            for path, info in result.items()
        ]

    def _send_request(self, method, url, files, params) -> Any:
        """Send a request to the ODP file storage service and return
        its JSON response."""
        try:
            r = requests.request(
                method,
                url,
                files=files,
                params=params,
                timeout=self.timeout,
            )
            r.raise_for_status()
            return r.json()

        except requests.RequestException as e:
            if e.response is not None:
                status_code = e.response.status_code
                try:
                    error_detail = e.response.json()['message']
                except (TypeError, ValueError, KeyError):
                    error_detail = e.response.text
            else:
                status_code = 503
                error_detail = str(e)

            raise HTTPException(status_code, error_detail) from e
