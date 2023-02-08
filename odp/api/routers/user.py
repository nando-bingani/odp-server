from functools import partial

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.auth import Authorize, Authorized
from odp.api.lib.paging import Page, Paginator
from odp.api.models import IdentityAuditModel, UserModel, UserModelIn
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import IdentityAudit, IdentityCommand, Role, User

router = APIRouter()


def create_audit_record(
        auth: Authorized,
        user: User,
        command: IdentityCommand,
) -> None:
    IdentityAudit(
        client_id=auth.client_id,
        user_id=auth.user_id,
        command=command,
        completed=True,
        _id=user.id,
        _email=user.email,
        _active=user.active,
        _roles=[role.id for role in user.roles],
    ).save()


def output_audit_model(row) -> IdentityAuditModel:
    return IdentityAuditModel(
        audit_id=row.IdentityAudit.id,
        client_id=row.IdentityAudit.client_id,
        client_user_id=row.IdentityAudit.user_id,
        client_user_name=row.user_name,
        command=row.IdentityAudit.command,
        completed=row.IdentityAudit.completed,
        error=row.IdentityAudit.error,
        timestamp=row.IdentityAudit.timestamp.isoformat(),
        user_id=row.IdentityAudit._id,
        user_email=row.IdentityAudit._email,
        user_active=row.IdentityAudit._active,
        user_roles=row.IdentityAudit._roles,
    )


@router.get(
    '/',
    response_model=Page[UserModel],
    dependencies=[Depends(Authorize(ODPScope.USER_READ))],
)
async def list_users(
        paginator: Paginator = Depends(partial(Paginator, sort='email')),
):
    return paginator.paginate(
        select(User),
        lambda row: UserModel(
            id=row.User.id,
            email=row.User.email,
            active=row.User.active,
            verified=row.User.verified,
            name=row.User.name,
            picture=row.User.picture,
            role_ids=[role.id for role in row.User.roles],
        )
    )


@router.get(
    '/{user_id}',
    response_model=UserModel,
    dependencies=[Depends(Authorize(ODPScope.USER_READ))],
)
async def get_user(
        user_id: str,
):
    if not (user := Session.get(User, user_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return UserModel(
        id=user.id,
        email=user.email,
        active=user.active,
        verified=user.verified,
        name=user.name,
        picture=user.picture,
        role_ids=[role.id for role in user.roles],
    )


@router.put(
    '/',
)
async def update_user(
        user_in: UserModelIn,
        auth: Authorized = Depends(Authorize(ODPScope.USER_ADMIN)),
):
    if not (user := Session.get(User, user_in.id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    user.active = user_in.active
    user.roles = [
        Session.get(Role, role_id)
        for role_id in user_in.role_ids
    ]
    user.save()
    create_audit_record(auth, user, IdentityCommand.edit)


@router.delete(
    '/{user_id}',
)
async def delete_user(
        user_id: str,
        auth: Authorized = Depends(Authorize(ODPScope.USER_ADMIN)),
):
    if not (user := Session.get(User, user_id)):
        raise HTTPException(HTTP_404_NOT_FOUND)

    try:
        create_audit_record(auth, user, IdentityCommand.delete)
        user.delete()

    except IntegrityError as e:
        raise HTTPException(
            HTTP_422_UNPROCESSABLE_ENTITY,
            'The user cannot be deleted due to associated tag instance data.',
        ) from e


@router.get(
    '/{user_id}/audit',
    response_model=Page[IdentityAuditModel],
    dependencies=[Depends(Authorize(ODPScope.USER_READ))],
)
async def get_user_audit_log(
        user_id: str,
        paginator: Paginator = Depends(partial(Paginator, sort='timestamp')),
):
    stmt = (
        select(IdentityAudit, User.name.label('user_name')).
        outerjoin(User, IdentityAudit.user_id == User.id).
        where(IdentityAudit._id == user_id)
    )

    return paginator.paginate(
        stmt,
        lambda row: output_audit_model(row),
    )


@router.get(
    '/{user_id}/audit/{audit_id}',
    response_model=IdentityAuditModel,
    dependencies=[Depends(Authorize(ODPScope.USER_READ))],
)
async def get_user_audit_detail(
        user_id: str,
        audit_id: int,
):
    if not (row := Session.execute(
            select(IdentityAudit, User.name.label('user_name')).
            outerjoin(User, IdentityAudit.user_id == User.id).
            where(IdentityAudit._id == user_id).
            where(IdentityAudit.id == audit_id)
    ).one_or_none()):
        raise HTTPException(HTTP_404_NOT_FOUND)

    return output_audit_model(row)
