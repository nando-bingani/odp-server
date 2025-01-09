import re
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, Authorized, TagAuthorize, UntagAuthorize
from odp.api.lib.paging import Paginator
from odp.api.lib.tagging import Tagger, output_tag_instance_model
from odp.api.models import PackageDetailModel, PackageModel, PackageModelIn, Page, TagInstanceModel, TagInstanceModelIn
from odp.api.routers.resource import output_resource_model
from odp.const import ODPScope
from odp.const.db import AuditCommand, PackageStatus, TagType
from odp.db import Session
from odp.db.models import Package, PackageAudit, PackageTag, PackageTagAudit

router = APIRouter()


def _package_key(title: str) -> str:
    """Calculate package key from title by replacing any non-word
    character with an underscore."""
    return re.sub(r'\W', '_', title)


def output_package_model(package: Package, *, detail=False) -> PackageModel | PackageDetailModel:
    cls = PackageDetailModel if detail else PackageModel
    record = next((r for r in package.records), None)
    kwargs = dict(
        id=package.id,
        key=package.key,
        title=package.title,
        status=package.status,
        timestamp=package.timestamp.isoformat(),
        provider_id=package.provider_id,
        provider_key=package.provider.key,
        resource_ids=[resource.id for resource in package.resources],
        record_id=record.id if record else None,
        record_doi=record.doi if record else None,
        record_sid=record.sid if record else None,
    )
    if detail:
        kwargs |= dict(
            resources=[
                output_resource_model(resource)
                for resource in package.resources
            ],
            tags=[
                output_tag_instance_model(package_tag)
                for package_tag in package.tags
            ],
        )

    return cls(**kwargs)


def create_audit_record(
        auth: Authorized,
        package: Package,
        timestamp: datetime,
        command: AuditCommand,
) -> None:
    PackageAudit(
        client_id=auth.client_id,
        user_id=auth.user_id,
        command=command,
        timestamp=timestamp,
        _id=package.id,
        _key=package.key,
        _title=package.title,
        _status=package.status,
        _provider_id=package.provider_id,
        _resources=[resource.id for resource in package.resources],
    ).save()


def create_tag_audit_record(
        auth: Authorized,
        package_tag: PackageTag,
        timestamp: datetime,
        command: AuditCommand,
) -> None:
    PackageTagAudit(
        client_id=auth.client_id,
        user_id=auth.user_id,
        command=command,
        timestamp=timestamp,
        _id=package_tag.id,
        _package_id=package_tag.package_id,
        _tag_id=package_tag.tag_id,
        _user_id=package_tag.user_id,
        _data=package_tag.data,
    ).save()


@router.get(
    '/',
    response_model=Page[PackageModel],
    description=f'List provider-accessible packages. Requires `{ODPScope.PACKAGE_READ}` scope.'
)
async def list_packages(
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_READ)),
        provider_id: str = None,
        paginator: Paginator = Depends(),
):
    stmt = select(Package)

    if auth.object_ids != '*':
        stmt = stmt.where(Package.provider_id.in_(auth.object_ids))

    if provider_id:
        auth.enforce_constraint([provider_id])
        stmt = stmt.where(Package.provider_id == provider_id)

    return paginator.paginate(
        stmt,
        lambda row: output_package_model(row.Package),
    )


@router.get(
    '/all/',
    response_model=Page[PackageModel],
    dependencies=[Depends(Authorize(ODPScope.PACKAGE_READ_ALL))],
    description=f'List all packages. Requires `{ODPScope.PACKAGE_READ_ALL}` scope.'
)
async def list_all_packages(
        provider_id: str = None,
        paginator: Paginator = Depends(),
):
    stmt = select(Package)

    if provider_id:
        stmt = stmt.where(Package.provider_id == provider_id)

    return paginator.paginate(
        stmt,
        lambda row: output_package_model(row.Package),
    )


@router.get(
    '/{package_id}',
    response_model=PackageDetailModel,
    description=f'Get a provider-accessible package. Requires `{ODPScope.PACKAGE_READ}` scope.'
)
async def get_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_READ)),
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    return output_package_model(package, detail=True)


@router.get(
    '/all/{package_id}',
    response_model=PackageDetailModel,
    dependencies=[Depends(Authorize(ODPScope.PACKAGE_READ_ALL))],
    description=f'Get any package. Requires `{ODPScope.PACKAGE_READ_ALL}` scope.'
)
async def get_any_package(
        package_id: str,
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_package_model(package, detail=True)


@router.post(
    '/',
    response_model=PackageDetailModel,
    description=f'Create a package. Requires access to the referenced provider. '
                f'Requires `{ODPScope.PACKAGE_WRITE}` scope.'
)
async def create_package(
        package_in: PackageModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
):
    return await _create_package(package_in, auth)


@router.post(
    '/admin/',
    response_model=PackageDetailModel,
    description=f'Create a package for any provider. Requires `{ODPScope.PACKAGE_ADMIN}` scope.'
)
async def admin_create_package(
        package_in: PackageModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_ADMIN)),
):
    return await _create_package(package_in, auth)


async def _create_package(
        package_in: PackageModelIn,
        auth: Authorized,
):
    auth.enforce_constraint([package_in.provider_id])

    package = Package(
        key=_package_key(package_in.title),
        title=package_in.title,
        status=PackageStatus.pending,
        timestamp=(timestamp := datetime.now(timezone.utc)),
        provider_id=package_in.provider_id,
    )
    package.save()
    create_audit_record(auth, package, timestamp, AuditCommand.insert)

    return output_package_model(package, detail=True)


@router.put(
    '/{package_id}',
    response_model=PackageDetailModel,
    description=f'Update a package. Requires access to the referenced provider (both existing '
                f'and new, if different). Requires `{ODPScope.PACKAGE_WRITE}` scope.'
)
async def update_package(
        package_id: str,
        package_in: PackageModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
):
    return await _update_package(package_id, package_in, auth)


@router.put(
    '/admin/{package_id}',
    response_model=PackageDetailModel,
    description=f'Update a package for any provider. Requires `{ODPScope.PACKAGE_ADMIN}` scope.'
)
async def admin_update_package(
        package_id: str,
        package_in: PackageModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_ADMIN)),
):
    return await _update_package(package_id, package_in, auth)


async def _update_package(
        package_id: str,
        package_in: PackageModelIn,
        auth: Authorized,
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id, package_in.provider_id])

    if (
            package.title != package_in.title or
            package.provider_id != package_in.provider_id
    ):
        # change the key only if the package has no resources,
        # as it is used in resource archival paths
        if package.title != package_in.title and not package.resources:
            package.key = _package_key(package_in.title)

        package.title = package_in.title
        package.timestamp = (timestamp := datetime.now(timezone.utc))
        package.provider_id = package_in.provider_id
        package.save()
        create_audit_record(auth, package, timestamp, AuditCommand.update)

    return output_package_model(package, detail=True)


@router.delete(
    '/{package_id}',
    description=f"Delete a package. Requires access to the package provider. "
                f"Requires `{ODPScope.PACKAGE_WRITE}` scope."
)
async def delete_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
):
    return await _delete_package(package_id, auth)


@router.delete(
    '/admin/{package_id}',
    description=f"Delete a package for any provider. Requires `{ODPScope.PACKAGE_ADMIN}` scope."
)
async def admin_delete_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_ADMIN)),
):
    return await _delete_package(package_id, auth)


async def _delete_package(
        package_id: str,
        auth: Authorized,
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    create_audit_record(auth, package, datetime.now(timezone.utc), AuditCommand.delete)

    try:
        package.delete()
    except IntegrityError as e:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, 'A package cannot be deleted if it is associated with a record.'
        ) from e


@router.post(
    '/{package_id}/tag',
)
async def tag_package(
        package_id: str,
        tag_instance_in: TagInstanceModelIn,
        auth: Authorized = Depends(TagAuthorize()),
) -> TagInstanceModel | None:
    """Set a tag instance on a package, returning the created
    or updated instance, or null if no change was made.

    Requires the scope associated with the tag.
    """
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    if package_tag := await Tagger(TagType.package).set_tag_instance(tag_instance_in, package, auth):
        return output_tag_instance_model(package_tag)


@router.delete(
    '/{package_id}/tag/{tag_instance_id}',
)
async def untag_package(
        package_id: str,
        tag_instance_id: str,
        auth: Authorized = Depends(UntagAuthorize(TagType.package)),
) -> None:
    """Remove a tag instance set by the calling user.

    Requires the scope associated with the tag.
    """
    await _untag_package(package_id, tag_instance_id, auth)


@router.delete(
    '/admin/{package_id}/tag/{tag_instance_id}',
)
async def admin_untag_package(
        package_id: str,
        tag_instance_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_ADMIN)),
) -> None:
    """Remove any tag instance from a package.

    Requires scope `odp.package:admin`.
    """
    await _untag_package(package_id, tag_instance_id, auth)


async def _untag_package(
        package_id: str,
        tag_instance_id: str,
        auth: Authorized,
) -> None:
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    await Tagger(TagType.package).delete_tag_instance(tag_instance_id, package, auth)
