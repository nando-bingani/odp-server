from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND

from odp.api.lib.auth import Authorize
from odp.api.lib.paging import Page, Paginator
from odp.api.models import PackageModel, PackageModelIn
from odp.api.routers.resource import output_resource_model
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Package

router = APIRouter()


def output_package_model(package: Package) -> PackageModel:
    return PackageModel(
        id=package.id,
        provider_id=package.provider_id,
        provider_key=package.provider.key,
        record_id=package.record_id,
        metadata=package.metadata_,
        timestamp=package.timestamp.isoformat(),
        resources=[
            output_resource_model(resource)
            for resource in package.resources
        ]
    )


@router.get(
    '/',
    response_model=Page[PackageModel],
    dependencies=[Depends(Authorize(ODPScope.PACKAGE_READ))],
)
async def list_packages(
        paginator: Paginator = Depends(),
):
    return paginator.paginate(
        select(Package),
        lambda row: output_package_model(row.Package),
    )


@router.get(
    '/{package_id}',
    response_model=PackageModel,
    dependencies=[Depends(Authorize(ODPScope.PACKAGE_READ))],
)
async def get_package(
        package_id: str,
):
    if not (package := Session.get(Package, package_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_package_model(package)


@router.post(
    '/',
    response_model=PackageModel,
    dependencies=[Depends(Authorize(ODPScope.PACKAGE_WRITE))],
)
async def create_package(
        package_in: PackageModelIn,
):
    package = Package(
        provider_id=package_in.provider_id,
        metadata_=package_in.metadata,
        timestamp=datetime.now(timezone.utc),
    )
