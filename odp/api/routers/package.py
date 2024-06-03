from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from jschon import JSON, JSONSchema
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, Authorized, TagAuthorize
from odp.api.lib.paging import Paginator
from odp.api.lib.schema import get_tag_schema
from odp.api.lib.utils import output_tag_instance_model
from odp.api.models import PackageDetailModel, PackageModel, PackageModelIn, Page, TagInstanceModel, TagInstanceModelIn
from odp.api.routers.resource import output_resource_model
from odp.const import ODPScope
from odp.const.db import AuditCommand, PackageStatus, TagCardinality, TagType
from odp.db import Session
from odp.db.models import Package, PackageAudit, PackageTag, PackageTagAudit, Resource, Tag

router = APIRouter()


def output_package_model(package: Package, *, detail=False) -> PackageModel | PackageDetailModel:
    cls = PackageDetailModel if detail else PackageModel
    record = next((r for r in package.records), None)
    kwargs = dict(
        id=package.id,
        title=package.title,
        status=package.status,
        notes=package.notes,
        timestamp=package.timestamp.isoformat(),
        provider_id=package.provider_id,
        provider_key=package.provider.key,
        resource_ids=[resource.id for resource in package.resources],
        record_id=record.id if record else None,
        record_doi=record.doi if record else None,
        record_sid=record.sid if record else None,
        tags=[
            output_tag_instance_model(package_tag)
            for package_tag in package.tags
        ],
    )
    if detail:
        kwargs |= dict(
            resources=[output_resource_model(resource) for resource in package.resources],
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
        _title=package.title,
        _status=package.status,
        _notes=package.notes,
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
    description=f'Create a package. Requires access to the referenced provider and to the providers '
                f'of all referenced resources. Requires `{ODPScope.PACKAGE_WRITE}` scope.'
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
    resources_in = [
        Session.get(Resource, resource_id)
        for resource_id in package_in.resource_ids
    ]
    auth.enforce_constraint(
        [package_in.provider_id] +
        [resource.provider_id for resource in resources_in]
    )

    package = Package(
        title=package_in.title,
        status=PackageStatus.pending,
        notes=package_in.notes,
        timestamp=(timestamp := datetime.now(timezone.utc)),
        provider_id=package_in.provider_id,
        resources=resources_in,
    )
    package.save()
    create_audit_record(auth, package, timestamp, AuditCommand.insert)

    return output_package_model(package, detail=True)


@router.put(
    '/{package_id}',
    response_model=PackageDetailModel,
    description=f'Update a package. Requires access to the referenced provider (both existing '
                f'and new, if different) and to the providers of all existing and newly referenced '
                f'resources. Requires `{ODPScope.PACKAGE_WRITE}` scope.'
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

    resources_in = [
        Session.get(Resource, resource_id)
        for resource_id in package_in.resource_ids
    ]
    auth.enforce_constraint(
        [package.provider_id, package_in.provider_id] +
        [resource.provider_id for resource in package.resources] +
        [resource.provider_id for resource in resources_in]
    )

    if (
            package.title != package_in.title or
            package.notes != package_in.notes or
            package.provider_id != package_in.provider_id or
            set(res.id for res in package.resources) != set(res_in.id for res_in in resources_in)
    ):
        package.title = package_in.title
        package.notes = package_in.notes
        package.timestamp = (timestamp := datetime.now(timezone.utc))
        package.provider_id = package_in.provider_id
        package.resources = resources_in
        package.save()
        create_audit_record(auth, package, timestamp, AuditCommand.update)

    return output_package_model(package, detail=True)


@router.delete(
    '/{package_id}',
    description=f"Delete a package. Requires access to the package provider and to the provider(s) "
                f"of the package's resources. Requires `{ODPScope.PACKAGE_WRITE}` scope."
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

    auth.enforce_constraint(
        [package.provider_id] +
        [resource.provider_id for resource in package.resources]
    )

    create_audit_record(auth, package, datetime.now(timezone.utc), AuditCommand.delete)

    try:
        package.delete()
    except IntegrityError as e:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, 'A package cannot be deleted if it is associated with a record.'
        ) from e


@router.post(
    '/{package_id}/tag',
    response_model=TagInstanceModel,
)
async def tag_package(
        package_id: str,
        tag_instance_in: TagInstanceModelIn,
        tag_schema: JSONSchema = Depends(get_tag_schema),
        auth: Authorized = Depends(TagAuthorize()),
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    if not (tag := Session.get(Tag, (tag_instance_in.tag_id, TagType.package))):
        raise HTTPException(HTTP_404_NOT_FOUND)

    # only one tag instance per package is allowed
    # update allowed only by the user who did the insert
    if tag.cardinality == TagCardinality.one:
        if package_tag := Session.execute(
                select(PackageTag).
                where(PackageTag.package_id == package_id).
                where(PackageTag.tag_id == tag_instance_in.tag_id)
        ).scalar_one_or_none():
            if package_tag.user_id != auth.user_id:
                raise HTTPException(HTTP_409_CONFLICT, 'Cannot update a tag set by another user')
            command = AuditCommand.update
        else:
            command = AuditCommand.insert

    # one tag instance per user per package is allowed
    # update a user's existing tag instance if found
    elif tag.cardinality == TagCardinality.user:
        if package_tag := Session.execute(
                select(PackageTag).
                where(PackageTag.package_id == package_id).
                where(PackageTag.tag_id == tag_instance_in.tag_id).
                where(PackageTag.user_id == auth.user_id)
        ).scalar_one_or_none():
            command = AuditCommand.update
        else:
            command = AuditCommand.insert

    # multiple tag instances are allowed per user per package
    # can only insert/delete
    elif tag.cardinality == TagCardinality.multi:
        command = AuditCommand.insert

    else:
        assert False

    if command == AuditCommand.insert:
        package_tag = PackageTag(
            package_id=package_id,
            tag_id=tag_instance_in.tag_id,
            tag_type=TagType.package,
            user_id=auth.user_id,
        )

    if package_tag.data != tag_instance_in.data:
        validity = tag_schema.evaluate(JSON(tag_instance_in.data)).output('detailed')
        if not validity['valid']:
            raise HTTPException(HTTP_422_UNPROCESSABLE_ENTITY, validity)

        package_tag.data = tag_instance_in.data
        package_tag.timestamp = (timestamp := datetime.now(timezone.utc))
        package_tag.save()

        package.timestamp = timestamp
        package.save()

        create_tag_audit_record(auth, package_tag, timestamp, command)

    return output_tag_instance_model(package_tag)
