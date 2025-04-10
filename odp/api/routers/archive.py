from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from starlette.status import HTTP_404_NOT_FOUND

from odp.api.lib.auth import Authorize
from odp.api.lib.paging import Paginator
from odp.api.models import ArchiveModel, Page
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Archive, ArchiveResource

router = APIRouter()


def output_archive_model(result) -> ArchiveModel:
    return ArchiveModel(
        id=result.Archive.id,
        type=result.Archive.type,
        scope_id=result.Archive.scope_id,
        upload_url=result.Archive.upload_url,
        download_url=result.Archive.download_url,
        resource_count=result.count,
    )


@router.get(
    '/',
    dependencies=[Depends(Authorize(ODPScope.ARCHIVE_READ))],
)
async def list_archives(
        paginator: Paginator = Depends(),
) -> Page[ArchiveModel]:
    """
    List all archive configurations. Requires scope `odp.archive:read`.
    """
    stmt = (
        select(Archive, func.count(ArchiveResource.archive_id)).
        outerjoin(ArchiveResource).
        group_by(Archive)
    )

    return paginator.paginate(
        stmt,
        lambda row: output_archive_model(row),
        sort_model=Archive,
    )


@router.get(
    '/{archive_id}',
    dependencies=[Depends(Authorize(ODPScope.ARCHIVE_READ))],
)
async def get_archive(
        archive_id: str,
) -> ArchiveModel:
    """
    Get an archive configuration. Requires scope `odp.archive:read`.
    """
    stmt = (
        select(Archive, func.count(ArchiveResource.archive_id)).
        outerjoin(ArchiveResource).
        group_by(Archive).
        where(Archive.id == archive_id)
    )

    if not (result := Session.execute(stmt).one_or_none()):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_archive_model(result)
