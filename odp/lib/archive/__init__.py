from collections import namedtuple
from os import PathLike
from typing import BinaryIO

ArchiveFileInfo = namedtuple('ArchiveFileInfo', (
    'path', 'size', 'sha256'
))


class ArchiveResponse:
    pass


class ArchiveFileResponse(ArchiveResponse):
    """TODO"""


class ArchiveRedirectResponse(ArchiveResponse):
    def __init__(self, redirect_url: str):
        self.redirect_url = redirect_url


class ArchiveError(Exception):
    def __init__(self, status_code, error_detail):
        self.status_code = status_code
        self.error_detail = error_detail


class ArchiveAdapter:
    """Abstract base class for an archive implementation adapter.

    All paths are relative.
    """

    def __init__(self, download_url: str | None, upload_url: str | None) -> None:
        self.download_url = download_url
        self.upload_url = upload_url

    async def get(self, path: str | PathLike) -> ArchiveFileResponse | ArchiveRedirectResponse:
        """Send the contents of the file at `path` to the client,
        or return a redirect."""
        raise NotImplementedError

    async def get_zip(self, *paths: str | PathLike) -> ArchiveFileResponse:
        """Send a zip file of the directories (recursively) and
        files at `paths` to the client."""
        raise NotImplementedError

    async def put(
            self,
            folder: str,
            filename: str,
            file: BinaryIO,
            sha256: str,
            unpack: bool,
    ) -> list[ArchiveFileInfo]:
        """Add or unpack `file` into `folder` relative to the
        archive's upload directory.

        Return a list of ArchiveFileInfo tuple(path, size, sha256)
        for each written file.
        """
        raise NotImplementedError
