from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize
from odp.api.lib.paging import Page, Paginator
from odp.api.models import ProviderModel, ProviderModelIn
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Provider

router = APIRouter()


def output_provider_model(provider: Provider) -> ProviderModel:
    return ProviderModel(
        id=provider.id,
        abbr=provider.abbr,
        name=provider.name,
        collection_ids=[collection.id for collection in provider.collections],
    )


@router.get(
    '/',
    response_model=Page[ProviderModel],
    dependencies=[Depends(Authorize(ODPScope.PROVIDER_READ))],
)
async def list_providers(
        paginator: Paginator = Depends(),
):
    return paginator.paginate(
        select(Provider),
        lambda row: output_provider_model(row.Provider),
    )


@router.get(
    '/{provider_id}',
    response_model=ProviderModel,
    dependencies=[Depends(Authorize(ODPScope.PROVIDER_READ))],
)
async def get_provider(
        provider_id: str,
):
    if not (provider := Session.get(Provider, provider_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_provider_model(provider)


@router.post(
    '/',
    response_model=ProviderModel,
    dependencies=[Depends(Authorize(ODPScope.PROVIDER_ADMIN))],
)
async def create_provider(
        provider_in: ProviderModelIn,
):
    if Session.execute(
            select(Provider).
            where(Provider.abbr == provider_in.abbr)
    ).first() is not None:
        raise HTTPException(HTTP_409_CONFLICT, 'Provider abbreviation is already in use')

    provider = Provider(
        abbr=provider_in.abbr,
        name=provider_in.name,
    )
    provider.save()

    return output_provider_model(provider)


@router.put(
    '/{provider_id}',
    dependencies=[Depends(Authorize(ODPScope.PROVIDER_ADMIN))],
)
async def update_provider(
        provider_id: str,
        provider_in: ProviderModelIn,
):
    if not (provider := Session.get(Provider, provider_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    if Session.execute(
            select(Provider).
            where(Provider.id != provider_id).
            where(Provider.abbr == provider_in.abbr)
    ).first() is not None:
        raise HTTPException(HTTP_409_CONFLICT, 'Provider abbreviation is already in use')

    provider.abbr = provider_in.abbr
    provider.name = provider_in.name
    provider.save()


@router.delete(
    '/{provider_id}',
    dependencies=[Depends(Authorize(ODPScope.PROVIDER_ADMIN))],
)
async def delete_provider(
        provider_id: str,
):
    if not (provider := Session.get(Provider, provider_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    try:
        provider.delete()
    except IntegrityError as e:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY,
            'A provider with non-empty collections cannot be deleted.',
        ) from e
