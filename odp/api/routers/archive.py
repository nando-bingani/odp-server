from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND

from odp.api.lib.auth import Authorize
from odp.api.lib.paging import Page, Paginator
from odp.api.models import ArchiveModel
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Archive

router = APIRouter()


@router.get(
    '/',
    response_model=Page[ArchiveModel],
    dependencies=[Depends(Authorize(ODPScope.ARCHIVE_READ))],
)
async def list_archives(
        paginator: Paginator = Depends(),
):
    return paginator.paginate(
        select(Archive),
        lambda row: ArchiveModel(
            id=row.Archive.id,
            url=row.Archive.url,
        )
    )


@router.get(
    '/{archive_id}',
    response_model=ArchiveModel,
    dependencies=[Depends(Authorize(ODPScope.ARCHIVE_READ))],
)
async def get_archive(
        archive_id: str,
):
    archive = Session.execute(
        select(Archive).
        where(Archive.id == archive_id)
    ).scalar_one_or_none()

    if not archive:
        raise HTTPException(HTTP_404_NOT_FOUND)

    return ArchiveModel(
        id=archive.id,
        url=archive.url,
    )
