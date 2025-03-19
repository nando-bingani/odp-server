from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from jschon import JSON, JSONPatch, URI
from jschon_translation import remove_empty_children
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, Authorized, TagAuthorize, UntagAuthorize
from odp.api.lib.paging import Paginator
from odp.api.lib.schema import get_metadata_validity
from odp.api.lib.tagging import Tagger, output_tag_instance_model
from odp.api.models import PackageDetailModel, PackageModel, PackageModelIn, Page, TagInstanceModel, TagInstanceModelIn
from odp.api.routers.resource import output_resource_model
from odp.const import ODPScope
from odp.const.db import PackageCommand, PackageStatus, SchemaType, TagType
from odp.db import Session
from odp.db.models import Package, PackageAudit, Provider, Schema
from odp.lib.schema import schema_catalog

router = APIRouter()


def output_package_model(package: Package, *, detail=False) -> PackageModel | PackageDetailModel:
    cls = PackageDetailModel if detail else PackageModel
    record = next((r for r in package.records), None)
    kwargs = dict(
        id=package.id,
        key=package.key,
        status=package.status,
        timestamp=package.timestamp.isoformat(),
        provider_id=package.provider_id,
        provider_key=package.provider.key,
        resource_ids=[resource.id for resource in package.resources],
        schema_id=package.schema_id,
        schema_uri=package.schema.uri,
        record_id=record.id if record else None,
        record_doi=record.doi if record else None,
        record_sid=record.sid if record else None,
        resources=[
            output_resource_model(resource)
            for resource in package.resources
        ],
        tags=[
            output_tag_instance_model(package_tag)
            for package_tag in package.tags
        ],
    )
    if detail:
        kwargs |= dict(
            metadata=package.metadata_,
            validity=package.validity,
        )

    return cls(**kwargs)


def create_audit_record(
        auth: Authorized,
        package: Package,
        timestamp: datetime,
        command: PackageCommand,
) -> None:
    PackageAudit(
        client_id=auth.client_id,
        user_id=auth.user_id,
        command=command,
        timestamp=timestamp,
        _id=package.id,
        _key=package.key,
        _status=package.status,
        _provider_id=package.provider_id,
        _schema_id=package.schema_id,
        _resources=[resource.id for resource in package.resources],
    ).save()


def ensure_status(package: Package, status: PackageStatus) -> None:
    if package.status != status:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY,
            f"Package status must be '{status}'"
        )


@router.get(
    '/',
)
async def list_packages(
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_READ)),
        provider_id: str = None,
        paginator: Paginator = Depends(),
) -> Page[PackageModel]:
    """
    List provider-accessible packages. Requires scope `odp.package:read`.
    """
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
    dependencies=[Depends(Authorize(ODPScope.PACKAGE_READ_ALL))],
)
async def list_all_packages(
        provider_id: str = None,
        paginator: Paginator = Depends(),
) -> Page[PackageModel]:
    """
    List all packages. Requires scope `odp.package:read_all`.
    """
    stmt = select(Package)

    if provider_id:
        stmt = stmt.where(Package.provider_id == provider_id)

    return paginator.paginate(
        stmt,
        lambda row: output_package_model(row.Package),
    )


@router.get(
    '/{package_id}',
)
async def get_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_READ)),
) -> PackageDetailModel:
    """
    Get a provider-accessible package. Requires scope `odp.package:read`.
    """
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    return output_package_model(package, detail=True)


@router.get(
    '/all/{package_id}',
    dependencies=[Depends(Authorize(ODPScope.PACKAGE_READ_ALL))],
)
async def get_any_package(
        package_id: str,
) -> PackageDetailModel:
    """
    Get any package. Requires scope `odp.package:read_all`.
    """
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_package_model(package, detail=True)


@router.post(
    '/',
)
async def create_package(
        package_in: PackageModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
) -> PackageDetailModel:
    """
    Create a provider-accessible package. Requires scope `odp.package:write`.
    """
    return await _create_package(package_in, auth)


@router.post(
    '/admin/',
)
async def admin_create_package(
        package_in: PackageModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_ADMIN)),
) -> PackageDetailModel:
    """
    Create a package for any provider. Requires scope `odp.package:admin`.
    """
    return await _create_package(package_in, auth)


async def _create_package(
        package_in: PackageModelIn,
        auth: Authorized,
):
    auth.enforce_constraint([package_in.provider_id])

    if not (provider := Session.get(Provider, package_in.provider_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    timestamp = datetime.now(timezone.utc)
    date = timestamp.strftime('%Y_%m_%d')
    n = 1
    while True:
        try:
            package = Package(
                key=f'{provider.key}_{date}_{n:03}',
                status=PackageStatus.pending,
                timestamp=timestamp,
                provider_id=package_in.provider_id,
                schema_id=package_in.schema_id,
                schema_type=SchemaType.metadata,
            )
            package.save()
            break
        except IntegrityError:
            Session.rollback()
            n += 1

    create_audit_record(auth, package, timestamp, PackageCommand.insert)

    return output_package_model(package, detail=True)


@router.put(
    '/admin/{package_id}',
)
async def admin_update_package(
        package_id: str,
        package_in: PackageModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_ADMIN)),
) -> PackageDetailModel:
    """
    Update any package. Requires scope `odp.package:admin`.
    """
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
            package.provider_id != package_in.provider_id or
            package.schema_id != package_in.schema_id
    ):
        package.timestamp = (timestamp := datetime.now(timezone.utc))
        package.provider_id = package_in.provider_id
        package.schema_id = package_in.schema_id
        package.save()
        create_audit_record(auth, package, timestamp, PackageCommand.update)

    return output_package_model(package, detail=True)


@router.delete(
    '/{package_id}',
)
async def delete_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
) -> None:
    """
    Delete a provider-accessible package. The package status must be `pending`.
    Requires scope `odp.package:write`.
    """
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    ensure_status(package, PackageStatus.pending)

    await _delete_package(package_id, auth)


@router.delete(
    '/admin/{package_id}',
)
async def admin_delete_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_ADMIN)),
) -> None:
    """
    Delete any package. Requires scope `odp.package:admin`.
    """
    await _delete_package(package_id, auth)


async def _delete_package(
        package_id: str,
        auth: Authorized,
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    create_audit_record(auth, package, datetime.now(timezone.utc), PackageCommand.delete)

    try:
        package.delete()
    except IntegrityError as e:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY,
            'A package with an associated record or resources cannot be deleted.',
        ) from e


@router.post(
    '/{package_id}/tag',
)
async def tag_package(
        package_id: str,
        tag_instance_in: TagInstanceModelIn,
        auth: Authorized = Depends(TagAuthorize()),
) -> TagInstanceModel | None:
    """
    Set a tag instance on a package, returning the created or updated instance,
    or null if no change was made. The package status must be `pending`. Requires
    the scope associated with the tag.
    """
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    ensure_status(package, PackageStatus.pending)

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
    """
    Remove a tag instance set by the calling user. The package status must be `pending`.
    Requires the scope associated with the tag.
    """
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    ensure_status(package, PackageStatus.pending)

    await _untag_package(package_id, tag_instance_id, auth)


@router.delete(
    '/admin/{package_id}/tag/{tag_instance_id}',
)
async def admin_untag_package(
        package_id: str,
        tag_instance_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_ADMIN)),
) -> None:
    """
    Remove any tag instance from a package. Requires scope `odp.package:admin`.
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


@router.post(
    '/{package_id}/submit',
)
async def submit_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
) -> None:
    """
    Submit a provider-accessible package. Requires scope `odp.package:write`.
    """
    await _submit_package(package_id, auth)


@router.post(
    '/admin/{package_id}/submit',
)
async def admin_submit_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_ADMIN)),
) -> None:
    """
    Submit any package. Requires scope `odp.package:admin`.
    """
    await _submit_package(package_id, auth)


async def _submit_package(
        package_id: str,
        auth: Authorized,
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    ensure_status(package, PackageStatus.pending)

    tag_patch = []
    for package_tag in package.tags:
        tag_schema = schema_catalog.get_schema(URI(package_tag.tag.schema.uri))
        tag_schema_result = tag_schema.evaluate(JSON(package_tag.data))
        tag_patch += tag_schema_result.output('translation-patch', scheme=package.schema_id)

    _schema = Session.get(Schema, (package.schema_id, package.schema_type))
    metadata_schema = schema_catalog.get_schema(URI(_schema.uri))
    metadata_template = schema_catalog.load_json(URI(_schema.template_uri))
    metadata = JSONPatch(*tag_patch).evaluate(metadata_template, resolve_array_inserts=True)
    remove_empty_children(metadata)

    package.metadata_ = metadata
    package.validity = await get_metadata_validity(package.metadata_, metadata_schema)
    package.status = PackageStatus.submitted
    package.timestamp = (timestamp := datetime.now(timezone.utc))
    package.save()

    create_audit_record(auth, package, timestamp, PackageCommand.submit)


@router.post(
    '/{package_id}/cancel',
)
async def cancel_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
) -> None:
    """
    Cancel submission of a provider-accessible package. Requires scope `odp.package:write`.
    """
    await _cancel_package(package_id, auth)


@router.post(
    '/admin/{package_id}/cancel',
)
async def admin_cancel_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_ADMIN)),
) -> None:
    """
    Cancel submission of any package. Requires scope `odp.package:admin`.
    """
    await _cancel_package(package_id, auth)


async def _cancel_package(
        package_id: str,
        auth: Authorized,
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    ensure_status(package, PackageStatus.submitted)

    package.status = PackageStatus.pending
    package.metadata_ = None
    package.validity = None
    package.timestamp = (timestamp := datetime.now(timezone.utc))
    package.save()

    create_audit_record(auth, package, timestamp, PackageCommand.cancel)
