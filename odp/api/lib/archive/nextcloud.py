from typing import Any
from urllib.parse import urljoin

import requests
from fastapi import HTTPException, UploadFile

from odp.api.lib.archive import ArchiveAdapter, FileInfo


class NextcloudArchiveAdapter(ArchiveAdapter):
    """Adapter for a Nextcloud archive."""

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

    @staticmethod
    def _send_request(method, url, files, params) -> Any:
        """Send a request to the ODP file storage service and return
        its JSON response."""
        try:
            r = requests.request(
                method,
                url,
                files=files,
                params=params,
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
