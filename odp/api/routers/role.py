from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, select_scopes
from odp.api.lib.paging import Paginator
from odp.api.models import Page, RoleModel, RoleModelIn
from odp.const import ODPScope
from odp.const.db import ScopeType
from odp.db import Session
from odp.db.models import Collection, Role

router = APIRouter()


def output_role_model(role: Role) -> RoleModel:
    return RoleModel(
        id=role.id,
        scope_ids=[scope.id for scope in role.scopes],
        collection_specific=role.collection_specific,
        collection_keys={collection.id: collection.key
                         for collection in role.collections} if role.collection_specific else {},
    )


@router.get(
    '/',
    response_model=Page[RoleModel],
    dependencies=[Depends(Authorize(ODPScope.ROLE_READ))],
)
async def list_roles(
        paginator: Paginator = Depends(),
):
    return paginator.paginate(
        select(Role),
        lambda row: output_role_model(row.Role),
    )


@router.get(
    '/{role_id}',
    response_model=RoleModel,
    dependencies=[Depends(Authorize(ODPScope.ROLE_READ))],
)
async def get_role(
        role_id: str,
):
    if not (role := Session.get(Role, role_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_role_model(role)


@router.post(
    '/',
    dependencies=[Depends(Authorize(ODPScope.ROLE_ADMIN))],
)
async def create_role(
        role_in: RoleModelIn,
):
    if Session.get(Role, role_in.id):
        raise HTTPException(
            HTTP_409_CONFLICT,
            'Role id is already in use',
        )

    if role_in.collection_specific and ODPScope.ROLE_ADMIN in role_in.scope_ids:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY,
            f'Scope {ODPScope.ROLE_ADMIN.value!r} cannot be granted to a collection-specific role.',
        )

    role = Role(
        id=role_in.id,
        scopes=select_scopes(role_in.scope_ids, [ScopeType.odp, ScopeType.client]),
        collection_specific=role_in.collection_specific,
        collections=[
            Session.get(Collection, collection_id)
            for collection_id in role_in.collection_ids
        ],
    )
    role.save()


@router.put(
    '/',
    dependencies=[Depends(Authorize(ODPScope.ROLE_ADMIN))],
)
async def update_role(
        role_in: RoleModelIn,
):
    if not (role := Session.get(Role, role_in.id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    if role_in.collection_specific and ODPScope.ROLE_ADMIN in role_in.scope_ids:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY,
            f'Scope {ODPScope.ROLE_ADMIN.value!r} cannot be granted to a collection-specific role.',
        )

    role.scopes = select_scopes(role_in.scope_ids, [ScopeType.odp, ScopeType.client])
    role.collection_specific = role_in.collection_specific
    role.collections = [
        Session.get(Collection, collection_id)
        for collection_id in role_in.collection_ids
    ]
    role.save()


@router.delete(
    '/{role_id}',
    dependencies=[Depends(Authorize(ODPScope.ROLE_ADMIN))],
)
async def delete_role(
        role_id: str,
):
    if not (role := Session.get(Role, role_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    role.delete()
