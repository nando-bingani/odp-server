from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Paginator
from odp.api.models import PackageDetailModel, PackageModel, PackageModelIn, Page
from odp.api.routers.resource import output_resource_model
from odp.const import ODPScope
from odp.const.db import PackageStatus
from odp.db import Session
from odp.db.models import Package, Resource

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
    )
    if detail:
        kwargs |= dict(
            resources=[output_resource_model(resource) for resource in package.resources],
        )

    return cls(**kwargs)


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
        auth.enforce_constraint([provider_id])
        stmt = stmt.where(Package.provider_id == provider_id)

    return paginator.paginate(
        stmt,
        lambda row: output_package_model(row.Package),
    )


@router.get(
    '/{package_id}',
    response_model=PackageDetailModel,
)
async def get_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_READ)),
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint([package.provider_id])

    return output_package_model(package, detail=True)


@router.post(
    '/',
    response_model=PackageModel,
    description=f'Create a package. The caller must have {ODPScope.PACKAGE_WRITE} access '
                f'with respect to the referenced provider and to the providers of all '
                f'referenced resources.'
)
async def create_package(
        package_in: PackageModelIn,
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
        title=package_in.title,
        status=PackageStatus.pending,
        notes=package_in.notes,
        timestamp=datetime.now(timezone.utc),
        provider_id=package_in.provider_id,
        resources=resources_in,
    )
    package.save()

    return output_package_model(package)


@router.put(
    '/{package_id}',
    response_model=PackageModel,
    description=f'Update a package. The caller must have {ODPScope.PACKAGE_WRITE} access '
                f'with respect to the referenced provider (both existing and new, if different) '
                f'and to the providers of all existing and newly referenced resources.'
)
async def update_package(
        package_id: str,
        package_in: PackageModelIn,
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
            package.title != package_in.title or
            package.notes != package_in.notes or
            package.provider_id != package_in.provider_id or
            set(res.id for res in package.resources) != set(res_in.id for res_in in resources_in)
    ):
        package.title = package_in.title
        package.notes = package_in.notes
        package.timestamp = datetime.now(timezone.utc)
        package.provider_id = package_in.provider_id
        package.resources = resources_in
        package.save()

    return output_package_model(package)


@router.delete(
    '/{package_id}',
    description=f"Delete a package. The caller must have {ODPScope.PACKAGE_WRITE} access "
                f"with respect to the provider of the package and to the provider(s) of "
                f"the package's resources."
)
async def delete_package(
        package_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PACKAGE_WRITE)),
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    auth.enforce_constraint(
        [package.provider_id] +
        [resource.provider_id for resource in package.resources]
    )

    try:
        package.delete()
    except IntegrityError as e:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY, 'A package cannot be deleted if it is associated with a record.'
        ) from e
