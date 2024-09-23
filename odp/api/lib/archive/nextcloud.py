from os import PathLike

from fastapi import UploadFile
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
        """Store the contents of the incoming `file` at `path` and
        verify the stored file against the given checksum."""

    async def put_zip(self, path: str | PathLike, file: UploadFile) -> None:
        """Unpack the contents of the incoming `file` into the
        directory at `path`."""
