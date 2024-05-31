from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Paginator
from odp.api.models import Page, ResourceModel, ResourceModelIn
from odp.const import ODPScope
from odp.const.db import AuditCommand
from odp.db import Session
from odp.db.models import ArchiveResource, PackageResource, Resource

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
    description=f'List provider-accessible resources. Requires `{ODPScope.RESOURCE_READ}` scope.'
)
async def list_resources(
        auth: Authorized = Depends(Authorize(ODPScope.RESOURCE_READ)),
        paginator: Paginator = Depends(),
        package_id: str = Query(None, title='Filter by package id'),
        provider_id: list[str] = Query(None, title='Filter by provider id(s)'),
        archive_id: str = Query(None, title='Only return resources stored in this archive'),
        exclude_archive_id: str = Query(None, title='Exclude resources stored in this archive'),
        exclude_packaged: bool = Query(False, title='Exclude resources associated with any package'),
):
    return await _list_resources(auth, paginator, package_id, provider_id, archive_id, exclude_archive_id, exclude_packaged)


@router.get(
    '/all/',
    response_model=Page[ResourceModel],
    description=f'List all resources. Requires `{ODPScope.RESOURCE_READ_ALL}` scope.'
)
async def list_all_resources(
        auth: Authorized = Depends(Authorize(ODPScope.RESOURCE_READ_ALL)),
        paginator: Paginator = Depends(),
        package_id: str = Query(None, title='Filter by package id'),
        provider_id: list[str] = Query(None, title='Filter by provider id(s)'),
        archive_id: str = Query(None, title='Only return resources stored in this archive'),
        exclude_archive_id: str = Query(None, title='Exclude resources stored in this archive'),
        exclude_packaged: bool = Query(False, title='Exclude resources associated with any package'),
):
    return await _list_resources(auth, paginator, package_id, provider_id, archive_id, exclude_archive_id, exclude_packaged)


async def _list_resources(
        auth: Authorized,
        paginator: Paginator,
        package_id: str,
        provider_id: list[str],
        archive_id: str,
        exclude_archive_id: str,
        exclude_packaged: bool,
):
    stmt = select(Resource)

    if auth.object_ids != '*':
        stmt = stmt.where(Resource.provider_id.in_(auth.object_ids))

    if package_id:
        stmt = stmt.join(PackageResource)
        stmt = stmt.where(PackageResource.package_id == package_id)

    if provider_id:
        auth.enforce_constraint(provider_id)
        stmt = stmt.where(Resource.provider_id.in_(provider_id))

    if archive_id:
        stmt = stmt.join(ArchiveResource)
        stmt = stmt.where(ArchiveResource.archive_id == archive_id)

    if exclude_archive_id:
        archived_subq = (
            select(ArchiveResource).
            where(ArchiveResource.resource_id == Resource.id).
            where(ArchiveResource.archive_id == exclude_archive_id)
        ).exists()
        stmt = stmt.where(~archived_subq)

    if exclude_packaged:
        packaged_subq = (
            select(PackageResource).
            where(PackageResource.resource_id == Resource.id)
        ).exists()
        stmt = stmt.where(~packaged_subq)

    return paginator.paginate(
        stmt,
        lambda row: output_resource_model(row.Resource),
    )


@router.get(
    '/{resource_id}',
    response_model=ResourceModel,
    description=f'Get a provider-accessible resource. Requires `{ODPScope.RESOURCE_READ}` scope.'
)
async def get_resource(
        resource_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.RESOURCE_READ)),
):
    if not (resource := Session.get(Resource, resource_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([resource.provider_id])

    return output_resource_model(resource)


@router.get(
    '/all/{resource_id}',
    response_model=ResourceModel,
    dependencies=[Depends(Authorize(ODPScope.RESOURCE_READ_ALL))],
    description=f'Get any resource. Requires `{ODPScope.RESOURCE_READ_ALL}` scope.'
)
async def get_any_resource(
        resource_id: str,
):
    if not (resource := Session.get(Resource, resource_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_resource_model(resource)


@router.post(
    '/',
    response_model=ResourceModel,
    description='Register a new resource. It is up to the caller to ensure that the '
                'resource is stored in the specified archive. Requires access to the '
                f'referenced provider. Requires `{ODPScope.RESOURCE_WRITE}` scope.',
)
async def create_resource(
        resource_in: ResourceModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.RESOURCE_WRITE)),
):
    return await _create_resource(resource_in, auth)


@router.post(
    '/admin/',
    response_model=ResourceModel,
    description='Register a new resource for any provider. It is up to the caller to '
                'ensure that the resource is stored in the specified archive. '
                f'Requires `{ODPScope.RESOURCE_ADMIN}` scope.',
)
async def admin_create_resource(
        resource_in: ResourceModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.RESOURCE_ADMIN)),
):
    return await _create_resource(resource_in, auth)


async def _create_resource(
        resource_in: ResourceModelIn,
        auth: Authorized,
):
    auth.enforce_constraint([resource_in.provider_id])

    if Session.execute(
            select(ArchiveResource).
            where(ArchiveResource.archive_id == resource_in.archive_id).
            where(ArchiveResource.path == resource_in.archive_path)
    ).first() is not None:
        raise HTTPException(
            HTTP_409_CONFLICT, f'path {resource_in.archive_path} '
                               f'already exists in archive {resource_in.archive_id}'
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
