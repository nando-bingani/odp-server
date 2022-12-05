from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Page, Paginator
from odp.api.models import ProviderModel, ProviderModelIn
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import AuditCommand, Provider, ProviderAudit

router = APIRouter()


def output_provider_model(provider: Provider) -> ProviderModel:
    return ProviderModel(
        id=provider.id,
        key=provider.key,
        name=provider.name,
        collection_keys={
            collection.key: collection.id
            for collection in provider.collections
        },
    )


def create_audit_record(
        auth: Authorized,
        provider: Provider,
        command: AuditCommand,
) -> None:
    ProviderAudit(
        client_id=auth.client_id,
        user_id=auth.user_id,
        command=command,
        timestamp=datetime.now(timezone.utc),
        _id=provider.id,
        _key=provider.key,
        _name=provider.name,
    ).save()


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
)
async def create_provider(
        provider_in: ProviderModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_ADMIN)),
):
    if Session.execute(
            select(Provider).
            where(Provider.key == provider_in.key)
    ).first() is not None:
        raise HTTPException(HTTP_409_CONFLICT, 'Provider key is already in use')

    provider = Provider(
        key=provider_in.key,
        name=provider_in.name,
    )
    provider.save()
    create_audit_record(auth, provider, AuditCommand.insert)

    return output_provider_model(provider)


@router.put(
    '/{provider_id}',
)
async def update_provider(
        provider_id: str,
        provider_in: ProviderModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_ADMIN)),
):
    if not (provider := Session.get(Provider, provider_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    if Session.execute(
            select(Provider).
            where(Provider.id != provider_id).
            where(Provider.key == provider_in.key)
    ).first() is not None:
        raise HTTPException(HTTP_409_CONFLICT, 'Provider key is already in use')

    if (
            provider.key != provider_in.key or
            provider.name != provider_in.name
    ):
        provider.key = provider_in.key
        provider.name = provider_in.name
        provider.save()
        create_audit_record(auth, provider, AuditCommand.update)


@router.delete(
    '/{provider_id}',
)
async def delete_provider(
        provider_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_ADMIN)),
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

    create_audit_record(auth, provider, AuditCommand.delete)
