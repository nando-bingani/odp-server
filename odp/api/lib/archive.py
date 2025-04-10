from fastapi import HTTPException
from starlette.status import HTTP_404_NOT_FOUND

from odp.const.db import ArchiveAdapter as Adapter
from odp.db import Session
from odp.db.models import Archive
from odp.lib.archive import ArchiveAdapter, filestore, website


async def get_archive_adapter(archive_id: str) -> ArchiveAdapter:
    if not (archive := Session.get(Archive, archive_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    adapter_cls = {
        Adapter.filestore: filestore.FilestoreArchiveAdapter,
        Adapter.website: website.WebsiteArchiveAdapter,
    }[archive.adapter]

    return adapter_cls(archive.download_url, archive.upload_url)
