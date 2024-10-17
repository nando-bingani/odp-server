import pathlib
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, File, HTTPException, Path, Query, UploadFile
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_405_METHOD_NOT_ALLOWED, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.archive import ArchiveAdapter, get_archive_adapter
from odp.api.lib.auth import ArchiveAuthorize, Authorize, Authorized
from odp.api.lib.paging import Paginator
from odp.api.models import ArchiveModel, ArchiveResourceModel, Page
from odp.const import ODPScope
from odp.const.db import HashAlgorithm
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
        hash=result.Resource.hash,
        hash_algorithm=result.Resource.hash_algorithm,
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
async def upload_file(
        archive_id: str,
        provider_id: str,
        package_id: str,
        path: str = Path(..., title='Resource path relative to the package root'),
        file: UploadFile = File(..., title='File upload'),
        unzip: bool = Query(False, title='Unzip uploaded file'),
        filename: str = Query(..., title='File name'),
        mimetype: str = Query(..., title='Content type'),
        sha256: str = Query(..., title='SHA-256 checksum'),
        title: str = Query(None, title='Resource title'),
        description: str = Query(None, title='Resource description'),
        archive_adapter: ArchiveAdapter = Depends(get_archive_adapter),
        provider_auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_READ)),
        package_auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
) -> None:
    """
    Upload a file to an archive and add it to a package.
    """
    if not (archive := Session.get(Archive, archive_id)):
        raise HTTPException(
            HTTP_404_NOT_FOUND, 'Archive not found'
        )

    provider_auth.enforce_constraint([provider_id])
    if not (provider := Session.get(Provider, provider_id)):
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

    if unzip:
        ...

    else:
        await _add_resource(
            archive,
            provider,
            package,
            path,
            file,
            filename,
            mimetype,
            sha256,
            title,
            description,
            archive_adapter,
        )


async def _add_resource(
        archive: Archive,
        provider: Provider,
        package: Package,
        path: str,
        file: UploadFile,
        filename: str,
        mimetype: str,
        sha256: str,
        title: str | None,
        description: str | None,
        archive_adapter: ArchiveAdapter,
):
    resource = Resource(
        title=title,
        description=description,
        filename=filename,
        mimetype=mimetype,
        size=file.size,
        hash=sha256,
        hash_algorithm=HashAlgorithm.sha256,
        timestamp=(timestamp := datetime.now(timezone.utc)),
        provider_id=provider.id,
    )
    resource.save()

    try:
        archive_resource = ArchiveResource(
            archive_id=archive.id,
            resource_id=resource.id,
            path=(archive_path := f'{provider.key}/{package.key}/{path}'),
            timestamp=timestamp,
        )
        archive_resource.save()
    except IntegrityError:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, f"Path '{archive_path}' already exists in archive"
        )

    try:
        package_resource = PackageResource(
            package_id=package.id,
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
            archive_path, file, sha256
        )
    except NotImplementedError:
        raise HTTPException(
            HTTP_405_METHOD_NOT_ALLOWED, f'Operation not supported for {archive.id}'
        )
