from os import PathLike
from urllib.parse import urljoin

import requests
from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse, RedirectResponse

from odp.api.lib.archive import ArchiveAdapter


class NextcloudArchiveAdapter(ArchiveAdapter):
    """Adapter for a Nextcloud archive."""

    async def get(self, path: str | PathLike) -> FileResponse | RedirectResponse:
        """Send the contents of the file at `path` to the client,
        or return a redirect to the relevant Nextcloud folder."""

    async def get_zip(self, *paths: str | PathLike) -> FileResponse:
        """Send a zip file of the directories and files at `paths`
        to the client."""

    async def put(self, path: str | PathLike, file: UploadFile, sha256: str) -> None:
        """Upload the incoming `file` to the ODP file storage service
        on the Nextcloud server, which in turn writes and verifies the
        file at `path` relative to the Nextcloud upload directory."""
        await file.seek(0)
        try:
            r = requests.post(
                urljoin(self.upload_url, path),
                files={'file': file.file},
                params={'sha256': sha256},
            )
            r.raise_for_status()

        except requests.RequestException as e:
            if e.response is not None:
                status_code = e.response.status_code
                error_detail = e.response.text
            else:
                status_code = 503
                error_detail = str(e)

            raise HTTPException(status_code, error_detail) from e
