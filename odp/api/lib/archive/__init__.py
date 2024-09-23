from os import PathLike

from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from starlette.status import HTTP_404_NOT_FOUND

from odp.const.db import ArchiveAdapter as Adapter
from odp.db import Session
from odp.db.models import Archive


class ArchiveAdapter:
    """Abstract base class for an archive implementation adapter.

    All paths are relative.
    """

    def __init__(self, url: str | PathLike) -> None:
        self.url = url

    async def get(self, path: str | PathLike) -> FileResponse | RedirectResponse:
        """Send the contents of the file at `path` to the client,
        or return a redirect."""
        raise NotImplementedError

    async def get_zip(self, *paths: str | PathLike) -> FileResponse:
        """Send a zip file of the directories (recursively) and
        files at `paths` to the client."""
        raise NotImplementedError

    async def put(self, path: str | PathLike, file: UploadFile, sha256: str) -> None:
        """Store the contents of the incoming `file` at `path` and
        verify the stored file against the given checksum."""
        raise NotImplementedError

    async def put_zip(self, path: str | PathLike, file: UploadFile) -> None:
        """Unpack the contents of the incoming `file` into the
        directory at `path`."""
        raise NotImplementedError


async def get_archive_adapter(archive_id: str) -> ArchiveAdapter:
    from .filesystem import FileSystemArchiveAdapter
    from .nextcloud import NextcloudArchiveAdapter
    from .website import WebsiteArchiveAdapter

    if not (archive := Session.get(Archive, archive_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    adapter_cls = {
        Adapter.filesystem: FileSystemArchiveAdapter,
        Adapter.nextcloud: NextcloudArchiveAdapter,
        Adapter.website: WebsiteArchiveAdapter,
    }[archive.adapter]

    return adapter_cls(archive.url)
