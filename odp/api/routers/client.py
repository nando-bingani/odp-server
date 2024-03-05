from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, hydra_admin_api, select_scopes
from odp.api.lib.paging import Page, Paginator
from odp.api.models import ClientModel, ClientModelIn
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Client

router = APIRouter()


def output_client_model(client: Client) -> ClientModel:
    hydra_client = hydra_admin_api.get_client(client.id)
    return ClientModel(
        id=client.id,
        name=hydra_client.name,
        scope_ids=[scope.id for scope in client.scopes],
        provider_specific=client.provider_specific,
        provider_id=client.provider_id,
        provider_key=client.provider.key if client.provider_id else None,
        grant_types=hydra_client.grant_types,
        response_types=hydra_client.response_types,
        redirect_uris=hydra_client.redirect_uris,
        post_logout_redirect_uris=hydra_client.post_logout_redirect_uris,
        token_endpoint_auth_method=hydra_client.token_endpoint_auth_method,
        allowed_cors_origins=hydra_client.allowed_cors_origins,
        client_credentials_grant_access_token_lifespan=hydra_client.client_credentials_grant_access_token_lifespan,
    )


def create_or_update_hydra_client(client_in: ClientModelIn) -> None:
    hydra_admin_api.create_or_update_client(
        id=client_in.id,
        name=client_in.name,
        secret=client_in.secret,
        scope_ids=client_in.scope_ids,
        grant_types=client_in.grant_types,
        response_types=client_in.response_types,
        redirect_uris=client_in.redirect_uris,
        post_logout_redirect_uris=client_in.post_logout_redirect_uris,
        token_endpoint_auth_method=client_in.token_endpoint_auth_method,
        allowed_cors_origins=client_in.allowed_cors_origins,
        client_credentials_grant_access_token_lifespan=client_in.client_credentials_grant_access_token_lifespan,
    )


@router.get(
    '/',
    response_model=Page[ClientModel],
    dependencies=[Depends(Authorize(ODPScope.CLIENT_READ))],
)
async def list_clients(
        paginator: Paginator = Depends(),
):
    return paginator.paginate(
        select(Client),
        lambda row: output_client_model(row.Client),
    )


@router.get(
    '/{client_id}',
    response_model=ClientModel,
    dependencies=[Depends(Authorize(ODPScope.CLIENT_READ))],
)
async def get_client(
        client_id: str,
):
    if not (client := Session.get(Client, client_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_client_model(client)


@router.post(
    '/',
    dependencies=[Depends(Authorize(ODPScope.CLIENT_ADMIN))],
)
async def create_client(
        client_in: ClientModelIn,
):
    if Session.get(Client, client_in.id):
        raise HTTPException(HTTP_409_CONFLICT, 'Client id is already in use')

    if client_in.secret is None:
        raise HTTPException(HTTP_422_UNPROCESSABLE_ENTITY, 'Client secret must be provided on create')

    if client_in.provider_specific and ODPScope.CLIENT_ADMIN in client_in.scope_ids:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY,
            f'Scope {ODPScope.CLIENT_ADMIN.value!r} cannot be granted to a provider-specific client.',
        )

    client = Client(
        id=client_in.id,
        scopes=select_scopes(client_in.scope_ids),
        provider_specific=client_in.provider_specific,
        provider_id=client_in.provider_id,
    )
    client.save()
    create_or_update_hydra_client(client_in)


@router.put(
    '/',
    dependencies=[Depends(Authorize(ODPScope.CLIENT_ADMIN))],
)
async def update_client(
        client_in: ClientModelIn,
):
    if not (client := Session.get(Client, client_in.id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    if client_in.provider_specific and ODPScope.CLIENT_ADMIN in client_in.scope_ids:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY,
            f'Scope {ODPScope.CLIENT_ADMIN.value!r} cannot be granted to a provider-specific client.',
        )

    client.scopes = select_scopes(client_in.scope_ids)
    client.provider_specific = client_in.provider_specific
    client.provider_id = client_in.provider_id
    client.save()
    create_or_update_hydra_client(client_in)


@router.delete(
    '/{client_id}',
    dependencies=[Depends(Authorize(ODPScope.CLIENT_ADMIN))],
)
async def delete_client(
        client_id: str,
):
    if not (client := Session.get(Client, client_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    client.delete()
    hydra_admin_api.delete_client(client_id)
