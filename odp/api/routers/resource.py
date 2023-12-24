from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Page, Paginator
from odp.api.models import ResourceModel, ResourceModelIn
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Resource, AuditCommand

router = APIRouter()


def output_resource_model(resource: Resource) -> ResourceModel:
    return ResourceModel(
        id=resource.id,
        provider_id=resource.provider_id,
        provider_key=resource.provider.key,
        archive_id=resource.archive_id,
        path=resource.path,
        type=resource.type,
        name=resource.name,
        size=resource.size,
        md5=resource.md5,
        timestamp=resource.timestamp.isoformat(),
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
):
    return paginator.paginate(
        select(Resource),
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
        raise HTTPException(HTTP_404_NOT_FOUND)

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
            select(Resource)
            .where(Resource.archive_id == resource_in.archive_id)
            .where(Resource.path == resource_in.path)
    ).first() is not None:
        raise HTTPException(HTTP_409_CONFLICT, 'path already exists in archive')

    resource = Resource(
        provider_id=resource_in.provider_id,
        archive_id=resource_in.archive_id,
        path=resource_in.path,
        type=resource_in.type,
        name=resource_in.name,
        size=resource_in.size,
        md5=resource_in.md5,
        timestamp=(timestamp := resource_in.timestamp or datetime.now(timezone.utc)),
        text_data=resource_in.text_data,
        binary_data=resource_in.binary_data,
    )
    resource.save()

    create_audit_record(auth, resource, timestamp, AuditCommand.insert)

    return output_resource_model(resource)
