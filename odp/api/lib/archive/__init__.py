from collections import namedtuple
from os import PathLike

from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from starlette.status import HTTP_404_NOT_FOUND

from odp.const.db import ArchiveAdapter as Adapter
from odp.db import Session
from odp.db.models import Archive

FileInfo = namedtuple('FileInfo', (
    'path', 'size', 'sha256'
))


class ArchiveAdapter:
    """Abstract base class for an archive implementation adapter.

    All paths are relative.
    """

    def __init__(self, download_url: str | None, upload_url: str | None) -> None:
        self.download_url = download_url
        self.upload_url = upload_url

    async def get(self, path: str | PathLike) -> FileResponse | RedirectResponse:
        """Send the contents of the file at `path` to the client,
        or return a redirect."""
        raise NotImplementedError

    async def get_zip(self, *paths: str | PathLike) -> FileResponse:
        """Send a zip file of the directories (recursively) and
        files at `paths` to the client."""
        raise NotImplementedError

    async def put(
            self,
            folder: str,
            filename: str,
            file: UploadFile,
            sha256: str,
            unpack: bool,
    ) -> list[FileInfo]:
        """Add or unpack `file` into `folder` relative to the
        archive's upload directory.

        Return a list of FileInfo tuple(path, size, sha256)
        for each written file.
        """
        raise NotImplementedError


async def get_archive_adapter(archive_id: str) -> ArchiveAdapter:
    from .filestore import FilestoreArchiveAdapter
    from .website import WebsiteArchiveAdapter

    if not (archive := Session.get(Archive, archive_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    adapter_cls = {
        Adapter.filestore: FilestoreArchiveAdapter,
        Adapter.website: WebsiteArchiveAdapter,
    }[archive.adapter]

    return adapter_cls(archive.download_url, archive.upload_url)
