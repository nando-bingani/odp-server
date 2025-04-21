from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Paginator
from odp.api.models import Page, ResourceModel
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import ArchiveResource, Package, Resource

router = APIRouter()


def output_resource_model(resource: Resource) -> ResourceModel:
    return ResourceModel(
        id=resource.id,
        title=resource.title,
        description=resource.description,
        path=resource.path,
        mimetype=resource.mimetype,
        size=resource.size,
        hash=resource.hash,
        hash_algorithm=resource.hash_algorithm,
        status=resource.status,
        timestamp=resource.timestamp.isoformat(),
        package_id=resource.package_id,
        package_key=resource.package.key,
        archive_paths={
            ar.archive_id: ar.path
            for ar in resource.archive_resources
        },
    )


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
):
    return await _list_resources(auth, paginator, package_id, provider_id, archive_id, exclude_archive_id)


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
):
    return await _list_resources(auth, paginator, package_id, provider_id, archive_id, exclude_archive_id)


async def _list_resources(
        auth: Authorized,
        paginator: Paginator,
        package_id: str,
        provider_id: list[str],
        archive_id: str,
        exclude_archive_id: str,
):
    stmt = select(Resource)
    join_package = False

    if auth.object_ids != '*':
        stmt = stmt.where(Package.provider_id.in_(auth.object_ids))
        join_package = True

    if package_id:
        stmt = stmt.where(Resource.package_id == package_id)

    if provider_id:
        auth.enforce_constraint([provider_id])
        stmt = stmt.where(Package.provider_id.in_(provider_id))
        join_package = True

    if join_package:
        stmt = stmt.join(Package)

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

    auth.enforce_constraint([resource.package.provider_id])

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
