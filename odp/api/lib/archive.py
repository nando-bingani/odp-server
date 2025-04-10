from fastapi import HTTPException
from starlette.status import HTTP_404_NOT_FOUND

from odp.const.db import ArchiveType
from odp.db import Session
from odp.db.models import Archive
from odp.lib.archive import ArchiveAdapter, filestore, website


async def get_archive_adapter(archive_id: str) -> ArchiveAdapter:
    if not (archive := Session.get(Archive, archive_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    adapter_cls = {
        ArchiveType.filestore: filestore.FilestoreArchiveAdapter,
        ArchiveType.website: website.WebsiteArchiveAdapter,
    }[archive.type]

    return adapter_cls(archive.download_url, archive.upload_url)
