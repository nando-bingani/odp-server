from datetime import datetime, timezone
from functools import partial

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, literal_column, select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Paginator
from odp.api.models import Page, ProviderAuditModel, ProviderDetailModel, ProviderModel, ProviderModelIn
from odp.const import ODPScope
from odp.const.db import AuditCommand
from odp.db import Session
from odp.db.models import Package, Provider, ProviderAudit, Resource, User

router = APIRouter()


def output_provider_model(
        result,
        *,
        detail=False,
) -> ProviderModel | ProviderDetailModel:
    cls = ProviderDetailModel if detail else ProviderModel

    kwargs = dict(
        id=result.Provider.id,
        key=result.Provider.key,
        name=result.Provider.name,
        package_count=result.package_count,
        collection_keys={
            collection.id: collection.key
            for collection in result.Provider.collections
        },
        timestamp=result.Provider.timestamp.isoformat(),
    )

    if detail:
        kwargs |= dict(
            user_names=(user_names := {
                user.id: user.name
                for user in result.Provider.users
            }),
            user_ids=list(user_names),
            client_ids=[
                client.id
                for client in result.Provider.clients
            ],
        )

    return cls(**kwargs)


def output_audit_model(row) -> ProviderAuditModel:
    return ProviderAuditModel(
        table='provider',
        tag_id=None,
        audit_id=row.ProviderAudit.id,
        client_id=row.ProviderAudit.client_id,
        user_id=row.ProviderAudit.user_id,
        user_name=row.user_name,
        command=row.ProviderAudit.command,
        timestamp=row.ProviderAudit.timestamp.isoformat(),
        provider_id=row.ProviderAudit._id,
        provider_key=row.ProviderAudit._key,
        provider_name=row.ProviderAudit._name,
        provider_users=row.ProviderAudit._users or [],
    )


def create_audit_record(
        auth: Authorized,
        provider: Provider,
        timestamp: datetime,
        command: AuditCommand,
) -> None:
    ProviderAudit(
        client_id=auth.client_id,
        user_id=auth.user_id,
        command=command,
        timestamp=timestamp,
        _id=provider.id,
        _key=provider.key,
        _name=provider.name,
        _users=[user.id for user in provider.users],
    ).save()


@router.get(
    '/',
)
async def list_providers(
        auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_READ)),
        paginator: Paginator = Depends(partial(Paginator, sort='key')),
) -> Page[ProviderModel]:
    """
    List providers accessible to the caller. Requires scope `odp.provider:read`.
    """
    return await _list_providers(auth, paginator)


@router.get(
    '/all/',
)
async def list_all_providers(
        auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_READ_ALL)),
        paginator: Paginator = Depends(partial(Paginator, sort='key')),
) -> Page[ProviderModel]:
    """
    List all providers. Requires scope `odp.provider:read_all`.
    """
    return await _list_providers(auth, paginator)


async def _list_providers(
        auth: Authorized,
        paginator: Paginator,
):
    stmt = (
        select(
            Provider,
            func.count(Package.id).label('package_count'),
        ).
        outerjoin(Package).
        group_by(Provider)
    )

    if auth.object_ids != '*':
        stmt = stmt.where(Provider.id.in_(auth.object_ids))

    return paginator.paginate(
        stmt,
        lambda row: output_provider_model(row),
        sort_model=Provider,
    )


@router.get(
    '/{provider_id}',
)
async def get_provider(
        provider_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_READ)),
) -> ProviderDetailModel:
    """
    Get a provider accessible to the caller. Requires scope `odp.provider:read`.
    """
    return await _get_provider(provider_id, auth)


@router.get(
    '/all/{provider_id}',
)
async def get_any_provider(
        provider_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_READ_ALL)),
) -> ProviderDetailModel:
    """
    Get any provider. Requires scope `odp.provider:read_all`.
    """
    return await _get_provider(provider_id, auth)


async def _get_provider(
        provider_id: str,
        auth: Authorized,
):
    auth.enforce_constraint([provider_id])

    stmt = (
        select(
            Provider,
            func.count(Package.id).label('package_count'),
        ).
        outerjoin(Package).
        where(Provider.id == provider_id).
        group_by(Provider)
    )

    if not (result := Session.execute(stmt).one_or_none()):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_provider_model(result, detail=True)


@router.post(
    '/',
)
async def create_provider(
        provider_in: ProviderModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_ADMIN)),
) -> ProviderModel:
    """
    Create a provider. Requires scope `odp.provider:admin`.
    """
    if Session.execute(
            select(Provider).
            where(Provider.key == provider_in.key)
    ).first() is not None:
        raise HTTPException(HTTP_409_CONFLICT, 'Provider key is already in use')

    provider = Provider(
        key=provider_in.key,
        name=provider_in.name,
        users=[
            Session.get(User, user_id)
            for user_id in provider_in.user_ids
        ],
        timestamp=(timestamp := datetime.now(timezone.utc)),
    )
    provider.save()
    create_audit_record(auth, provider, timestamp, AuditCommand.insert)

    result = Session.execute(
        select(
            Provider,
            literal_column('0').label('package_count'),
        ).
        where(Provider.id == provider.id)
    ).first()

    return output_provider_model(result)


@router.put(
    '/{provider_id}',
)
async def update_provider(
        provider_id: str,
        provider_in: ProviderModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_ADMIN)),
) -> None:
    """
    Update a provider. Requires scope `odp.provider:admin`.
    """
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
            provider.name != provider_in.name or
            set(user.id for user in provider.users) != set(provider_in.user_ids)
    ):
        provider.key = provider_in.key
        provider.name = provider_in.name
        provider.users = [
            Session.get(User, user_id)
            for user_id in provider_in.user_ids
        ]
        provider.timestamp = (timestamp := datetime.now(timezone.utc))
        provider.save()
        create_audit_record(auth, provider, timestamp, AuditCommand.update)


@router.delete(
    '/{provider_id}',
)
async def delete_provider(
        provider_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.PROVIDER_ADMIN)),
) -> None:
    """
    Delete a provider. Requires scope `odp.provider:admin`.
    """
    if not (provider := Session.get(Provider, provider_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    create_audit_record(auth, provider, datetime.now(timezone.utc), AuditCommand.delete)

    try:
        provider.delete()
    except IntegrityError as e:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY,
            'A provider with associated packages and/or resources cannot be deleted.',
        ) from e


@router.get(
    '/{provider_id}/audit',
    dependencies=[Depends(Authorize(ODPScope.PROVIDER_READ_ALL))],
)
async def get_provider_audit_log(
        provider_id: str,
        paginator: Paginator = Depends(partial(Paginator, sort='timestamp')),
) -> Page[ProviderAuditModel]:
    """
    Get a provider audit log. Requires scope `odp.provider:read_all`.
    """
    stmt = (
        select(ProviderAudit, User.name.label('user_name')).
        outerjoin(User, ProviderAudit.user_id == User.id).
        where(ProviderAudit._id == provider_id)
    )

    return paginator.paginate(
        stmt,
        lambda row: output_audit_model(row),
    )


@router.get(
    '/{provider_id}/audit/{audit_id}',
    dependencies=[Depends(Authorize(ODPScope.PROVIDER_READ_ALL))],
)
async def get_provider_audit_detail(
        provider_id: str,
        audit_id: int,
) -> ProviderAuditModel:
    """
    Get a provider audit log entry. Requires scope `odp.provider:read_all`.
    """
    if not (row := Session.execute(
            select(ProviderAudit, User.name.label('user_name')).
            outerjoin(User, ProviderAudit.user_id == User.id).
            where(ProviderAudit.id == audit_id).
            where(ProviderAudit._id == provider_id)
    ).one_or_none()):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_audit_model(row)
