from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Page, Paginator
from odp.api.models import ResourceModel, ResourceModelIn
from odp.const import ODPScope
from odp.const.db import AuditCommand
from odp.db import Session
from odp.db.models import ArchiveResource, Resource

router = APIRouter()


def output_resource_model(resource: Resource) -> ResourceModel:
    return ResourceModel(
        id=resource.id,
        title=resource.title,
        description=resource.description,
        filename=resource.filename,
        mimetype=resource.mimetype,
        size=resource.size,
        md5=resource.md5,
        timestamp=resource.timestamp.isoformat(),
        provider_id=resource.provider_id,
        provider_key=resource.provider.key,
        archive_urls={
            ar.archive_id: ar.archive.url + ar.path
            for ar in resource.archive_resources
        }
    )


def create_audit_record(
        auth: Authorized,
        resource: Resource,
        timestamp: datetime,
        command: AuditCommand,
) -> None:
    """TODO"""


@router.get(
    '/',
    response_model=Page[ResourceModel],
    dependencies=[Depends(Authorize(ODPScope.RESOURCE_READ))],
)
async def list_resources(
        paginator: Paginator = Depends(),
        archive_id: str = None,
):
    stmt = select(Resource)
    if archive_id:
        stmt = stmt.join(ArchiveResource)
        stmt = stmt.where(ArchiveResource.archive_id == archive_id)

    return paginator.paginate(
        stmt,
        lambda row: output_resource_model(row.Resource),
    )


@router.get(
    '/{resource_id}',
    response_model=ResourceModel,
    dependencies=[Depends(Authorize(ODPScope.RESOURCE_READ))],
)
async def get_resource(
        resource_id: str,
):
    if not (resource := Session.get(Resource, resource_id)):
        raise HTTPException(
            HTTP_404_NOT_FOUND, 'Unknown resource id'
        )

    return output_resource_model(resource)


@router.post(
    '/',
    response_model=ResourceModel,
)
async def create_resource(
        resource_in: ResourceModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.RESOURCE_WRITE)),
):
    if Session.execute(
            select(ArchiveResource).
            where(ArchiveResource.archive_id == resource_in.archive_id).
            where(ArchiveResource.path == resource_in.archive_path)
    ).first() is not None:
        raise HTTPException(
            HTTP_409_CONFLICT, 'path already exists in archive'
        )

    resource = Resource(
        title=resource_in.title,
        description=resource_in.description,
        filename=resource_in.filename,
        mimetype=resource_in.mimetype,
        size=resource_in.size,
        md5=resource_in.md5,
        timestamp=(timestamp := datetime.now(timezone.utc)),
        provider_id=resource_in.provider_id,
    )
    resource.save()

    archive_resource = ArchiveResource(
        archive_id=resource_in.archive_id,
        resource_id=resource.id,
        path=resource_in.archive_path,
        timestamp=timestamp,
    )
    archive_resource.save()

    create_audit_record(auth, resource, timestamp, AuditCommand.insert)

    return output_resource_model(resource)
