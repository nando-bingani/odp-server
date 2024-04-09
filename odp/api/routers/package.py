from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from jschon import JSONSchema
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Page, Paginator
from odp.api.lib.schema import get_metadata_validity, get_package_schema
from odp.api.models import PackageModel, PackageModelIn
from odp.const import ODPScope
from odp.const.db import SchemaType
from odp.db import Session
from odp.db.models import Package, Resource

router = APIRouter()


def output_package_model(package: Package) -> PackageModel:
    record = next((r for r in package.records), None)
    return PackageModel(
        id=package.id,
        provider_id=package.provider_id,
        provider_key=package.provider.key,
        schema_id=package.schema_id,
        metadata=package.metadata_,
        validity=package.validity,
        notes=package.notes,
        timestamp=package.timestamp.isoformat(),
        resource_ids=[resource.id for resource in package.resources],
        record_id=record.id if record else None,
        record_doi=record.doi if record else None,
        record_sid=record.sid if record else None,
    )


@router.get(
    '/',
    response_model=Page[PackageModel],
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
        stmt = stmt.where(Package.provider_id == provider_id)

    return paginator.paginate(
        stmt,
        lambda row: output_package_model(row.Package),
    )


@router.get(
    '/{package_id}',
    response_model=PackageModel,
)
async def get_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_READ)),
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    return output_package_model(package)


@router.post(
    '/',
    response_model=PackageModel,
)
async def create_package(
        package_in: PackageModelIn,
        metadata_schema: JSONSchema = Depends(get_package_schema),
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
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
        provider_id=package_in.provider_id,
        schema_id=package_in.schema_id,
        schema_type=SchemaType.metadata,
        metadata_=package_in.metadata,
        validity=await get_metadata_validity(package_in.metadata, metadata_schema),
        notes=package_in.notes,
        resources=resources_in,
        timestamp=datetime.now(timezone.utc),
    )
    package.save()

    return output_package_model(package)


@router.put(
    '/{package_id}',
    response_model=PackageModel,
)
async def update_package(
        package_id: str,
        package_in: PackageModelIn,
        metadata_schema: JSONSchema = Depends(get_package_schema),
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
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
            package.provider_id != package_in.provider_id or
            package.schema_id != package_in.schema_id or
            package.metadata_ != package_in.metadata or
            package.notes != package_in.notes or
            set(res.id for res in package.resources) != set(res_in.id for res_in in resources_in)
    ):
        package.provider_id = package_in.provider_id
        package.schema_id = package_in.schema_id
        package.metadata_ = package_in.metadata
        package.validity = await get_metadata_validity(package_in.metadata, metadata_schema)
        package.notes = package_in.notes
        package.resources = resources_in
        package.timestamp = datetime.now(timezone.utc)
        package.save()

    return output_package_model(package)
