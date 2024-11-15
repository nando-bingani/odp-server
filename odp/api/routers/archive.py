import mimetypes
import pathlib
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, File, HTTPException, Path, Query, UploadFile
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_405_METHOD_NOT_ALLOWED, HTTP_422_UNPROCESSABLE_ENTITY
from werkzeug.utils import secure_filename

from odp.api.lib.archive import ArchiveAdapter, get_archive_adapter
from odp.api.lib.auth import ArchiveAuthorize, Authorize, Authorized
from odp.api.lib.paging import Paginator
from odp.api.models import ArchiveModel, Page
from odp.const import ODPScope
from odp.const.db import HashAlgorithm
from odp.db import Session
from odp.db.models import Archive, ArchiveResource, Package, Provider, Resource

router = APIRouter()


def output_archive_model(result) -> ArchiveModel:
    return ArchiveModel(
        id=result.Archive.id,
        url=result.Archive.url,
        adapter=result.Archive.adapter,
        scope_id=result.Archive.scope_id,
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


@router.put(
    '/{archive_id}/{provider_id}/{package_id}/{folder:path}',
    dependencies=[Depends(ArchiveAuthorize())],
)
async def upload_file(
        archive_id: str,
        provider_id: str,
        package_id: str,
        folder: str = Path(..., title='Path to containing folder relative to package root'),
        unpack: bool = Query(False, title='Unpack zip file into folder'),
        file: UploadFile = File(..., title='File upload'),
        filename: str = Query(..., title='File name'),
        sha256: str = Query(..., title='SHA-256 checksum'),
        title: str = Query(None, title='Resource title'),
        description: str = Query(None, title='Resource description'),
        archive_adapter: ArchiveAdapter = Depends(get_archive_adapter),
        provider_auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_READ)),
        package_auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
) -> None:
    """
    Upload a file to an archive and add/unpack it into a package folder.

    By default, a single resource is created and associated with the archive
    and the package. If unpack is true and the file is a supported zip format,
    its contents are unpacked into the folder and, for each unpacked file, a
    resource is created and similarly associated.
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

    if '..' in folder:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, "'..' not allowed in folder"
        )

    if pathlib.Path(folder).is_absolute():
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, 'folder must be relative'
        )

    if not (filename := secure_filename(filename)):
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, 'invalid filename'
        )

    archive_folder = f'{provider.key}/{package.key}/{folder}'
    try:
        file_info_list = await archive_adapter.put(
            archive_folder, filename, file, sha256, unpack
        )
    except NotImplementedError:
        raise HTTPException(
            HTTP_405_METHOD_NOT_ALLOWED, f'Operation not supported for {archive.id}'
        )

    for file_info in file_info_list:
        res_mimetype, encoding = mimetypes.guess_type(file_info.relpath, strict=False)
        package_path = file_info.relpath.removeprefix(f'{provider.key}/{package.key}/')
        archive_path = file_info.relpath
        res_filename = pathlib.Path(file_info.relpath).name
        res_size = file_info.size
        res_hash = file_info.sha256
        res_title = title
        res_description = description

        resource = Resource(
            title=res_title,
            description=res_description,
            filename=res_filename,
            mimetype=res_mimetype,
            size=res_size,
            hash=res_hash,
            hash_algorithm=HashAlgorithm.sha256,
            timestamp=(timestamp := datetime.now(timezone.utc)),
            provider_id=provider.id,
        )
        resource.save()

        try:
            archive_resource = ArchiveResource(
                archive_id=archive.id,
                resource_id=resource.id,
                path=archive_path,
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
                path=package_path,
                timestamp=timestamp,
            )
            package_resource.save()
        except IntegrityError:
            raise HTTPException(
                HTTP_422_UNPROCESSABLE_ENTITY, f"Path '{package_path}' already exists in package"
            )
