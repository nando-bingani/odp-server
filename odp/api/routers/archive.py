import pathlib
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, File, HTTPException, Path, Query, UploadFile
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_405_METHOD_NOT_ALLOWED, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.archive import ArchiveAdapter, get_archive_adapter
from odp.api.lib.auth import ArchiveAuthorize, Authorize, Authorized
from odp.api.lib.paging import Paginator
from odp.api.models import ArchiveModel, ArchiveResourceModel, Page, ResourceModel
from odp.api.routers.resource import output_resource_model
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Archive, ArchiveResource, Package, PackageResource, Provider, Resource

router = APIRouter()


def output_archive_model(result) -> ArchiveModel:
    return ArchiveModel(
        id=result.Archive.id,
        url=result.Archive.url,
        adapter=result.Archive.adapter,
        scope_id=result.Archive.scope_id,
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


@router.get(
    '/{archive_id}/resources',
    dependencies=[Depends(Authorize(ODPScope.ARCHIVE_READ))],
)
async def list_resources(
        archive_id: str,
        paginator: Paginator = Depends(),
) -> Page[ArchiveResourceModel]:
    """
    List the resources in an archive. Requires scope `odp.archive:read`.
    """
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


@router.put(
    '/{archive_id}/{provider_id}/{package_id}/{path:path}',
    dependencies=[Depends(ArchiveAuthorize())],
)
async def upload_resource(
        archive_id: str,
        provider_id: str,
        package_id: str,
        path: str = Path(..., title='Resource path relative to the package root'),
        title: str = Query(..., title='Resource title'),
        description: str = Query(None, title='Resource description'),
        file: UploadFile = File(..., title='File upload'),
        filename: str = Query(..., title='File name'),
        mimetype: str = Query(..., title='Content type'),
        md5: str = Query(..., title='MD5 checksum'),
        archive_adapter: ArchiveAdapter = Depends(get_archive_adapter),
        provider_auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_READ)),
        package_auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
) -> ResourceModel:
    """
    Upload a file to an archive and add it to a package.
    """
    if not Session.get(Archive, archive_id):
        raise HTTPException(
            HTTP_404_NOT_FOUND, 'Archive not found'
        )

    provider_auth.enforce_constraint([provider_id])
    if not Session.get(Provider, provider_id):
        raise HTTPException(
            HTTP_404_NOT_FOUND, 'Provider not found'
        )

    if not (package := Session.get(Package, package_id)):
        raise HTTPException(
            HTTP_404_NOT_FOUND, 'Package not found'
        )
    package_auth.enforce_constraint([package.provider_id])

    if not path:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, 'path cannot be blank'
        )

    if '..' in path:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, "'..' not allowed in path"
        )

    if pathlib.Path(path).is_absolute():
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, 'path must be relative'
        )

    resource = Resource(
        title=title,
        description=description,
        filename=filename,
        mimetype=mimetype,
        size=file.size,
        md5=md5,
        timestamp=(timestamp := datetime.now(timezone.utc)),
        provider_id=provider_id,
    )
    resource.save()

    try:
        archive_resource = ArchiveResource(
            archive_id=archive_id,
            resource_id=resource.id,
            path=(archive_path := f'{provider_id}/{package_id}/{path}'),
            timestamp=timestamp,
        )
        archive_resource.save()
    except IntegrityError:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, f"Path '{archive_path}' already exists in archive"
        )

    try:
        package_resource = PackageResource(
            package_id=package_id,
            resource_id=resource.id,
            path=path,
            timestamp=timestamp,
        )
        package_resource.save()
    except IntegrityError:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, f"Path '{path}' already exists in package"
        )

    try:
        await archive_adapter.put(
            archive_path, file, md5
        )
    except NotImplementedError:
        raise HTTPException(
            HTTP_405_METHOD_NOT_ALLOWED, f'Operation not supported for {archive_id}'
        )

    return output_resource_model(resource)
