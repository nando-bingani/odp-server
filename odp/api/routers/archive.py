from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from starlette.status import HTTP_404_NOT_FOUND

from odp.api.lib.auth import Authorize
from odp.api.lib.paging import Page, Paginator
from odp.api.models import ArchiveModel, ArchiveResourceModel
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Archive, ArchiveResource, Resource

router = APIRouter()


def output_archive_model(result) -> ArchiveModel:
    return ArchiveModel(
        id=result.Archive.id,
        url=result.Archive.url,
        resource_count=result.count,
    )


def output_archive_resource_model(result) -> ArchiveResourceModel:
    return ArchiveResourceModel(
        archive_id=result.ArchiveResource.archive_id,
        resource_id=result.ArchiveResource.resource_id,
        path=result.ArchiveResource.path,
        title=result.Resource.title,
        description=result.Resource.description,
        filename=result.Resource.filename,
        mimetype=result.Resource.mimetype,
        size=result.Resource.size,
        md5=result.Resource.md5,
        timestamp=result.Resource.timestamp.isoformat(),
        provider_id=result.Resource.provider_id,
        provider_key=result.Resource.provider.key,
    )


@router.get(
    '/',
    response_model=Page[ArchiveModel],
    dependencies=[Depends(Authorize(ODPScope.ARCHIVE_READ))],
)
async def list_archives(
        paginator: Paginator = Depends(),
):
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
    response_model=ArchiveModel,
    dependencies=[Depends(Authorize(ODPScope.ARCHIVE_READ))],
)
async def get_archive(
        archive_id: str,
):
    stmt = (
        select(Archive, func.count(ArchiveResource.archive_id)).
        outerjoin(ArchiveResource).
        group_by(Archive).
        where(Archive.id == archive_id)
    )

    if not (result := Session.execute(stmt).one_or_none()):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_archive_model(result)


@router.get(
    '/{archive_id}/resources',
    response_model=Page[ArchiveResourceModel],
    dependencies=[Depends(Authorize(ODPScope.ARCHIVE_READ))],
)
async def list_resources(
        archive_id: str,
        paginator: Paginator = Depends(),
):
    if not Session.get(Archive, archive_id):
        raise HTTPException(HTTP_404_NOT_FOUND)

    stmt = (
        select(ArchiveResource, Resource).join(Resource).
        where(ArchiveResource.archive_id == archive_id)
    )

    return paginator.paginate(
        stmt,
        lambda row: output_archive_resource_model(row),
    )
