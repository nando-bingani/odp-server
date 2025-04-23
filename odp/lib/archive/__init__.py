from __future__ import annotations

from collections import namedtuple
from os import PathLike
from typing import BinaryIO

from odp.const.db import ArchiveType
from odp.db.models import Archive

ArchiveFileInfo = namedtuple('ArchiveFileInfo', (
    'path', 'size', 'sha256'
))


class ArchiveResponse:
    pass


class ArchiveFileResponse(ArchiveResponse):
    def __init__(self, file: BinaryIO):
        self.file = file


class ArchiveRedirectResponse(ArchiveResponse):
    def __init__(self, redirect_url: str):
        self.redirect_url = redirect_url


class ArchiveError(Exception):
    def __init__(self, status_code: int, error_detail: str):
        self.status_code = status_code
        self.error_detail = error_detail


class ArchiveAdapter:
    """Abstract base class for an archive implementation adapter.

    All paths are relative.
    """

    _instance_cache: dict[str, ArchiveAdapter] = {}

    @classmethod
    def get_instance(cls, archive: Archive) -> ArchiveAdapter:
        from . import filestore, website

        try:
            return cls._instance_cache[archive.id]
        except KeyError:
            pass

        adapter_cls = {
            ArchiveType.filestore: filestore.FilestoreArchiveAdapter,
            ArchiveType.website: website.WebsiteArchiveAdapter,
        }[archive.type]

        cls._instance_cache[archive.id] = instance = adapter_cls(archive.download_url, archive.upload_url)

        return instance

    def __init__(
            self,
            download_url: str | None,
            upload_url: str | None,
    ) -> None:
        self.download_url = download_url
        self.upload_url = upload_url

    async def get(
            self,
            path: str | PathLike,
    ) -> ArchiveResponse:
        """Return the contents of the file at `path`, or a redirect."""
        raise NotImplementedError

    async def get_zip(
            self,
            *paths: str | PathLike,
    ) -> ArchiveFileResponse:
        """Return a zip file of the directories (recursively) and
        files at `paths`."""
        raise NotImplementedError

    async def put(
            self,
            path: str,
            file: BinaryIO,
            sha256: str,
            unpack: bool,
    ) -> list[ArchiveFileInfo]:
        """Store `file` at `path` relative to the upload URL.

        If `unpack` is true, `file` is unzipped at the parent of `path`.

        Return a list of ArchiveFileInfo tuple(path, size, sha256)
        for each written file.
        """
        raise NotImplementedError

    async def delete(
            self,
            path: str | PathLike,
    ) -> None:
        """Delete the file at `path`."""
        raise NotImplementedError
