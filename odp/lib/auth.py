from odp.api.models.auth import Permission, Permissions, UserInfo
from odp.const import ODPScope
from odp.db import Session
from odp.db.models import Client, User
from odp.lib import exceptions as x


def get_client_permissions(client_id: str) -> Permissions:
    """Return effective client permissions."""

    def permission(scope_id: str) -> Permission:
        if client.provider_specific and ODPScope(scope_id).constrainable_by == 'provider':
            return [client.provider_id]
        return '*'

    if not (client := Session.get(Client, client_id)):
        raise x.ODPClientNotFound

    return {
        scope.id: permission(scope.id)
        for scope in client.scopes
    }


def get_user_permissions(user_id: str, client_id: str) -> Permissions:
    """Return effective user permissions, which may be linked with
    a user's access token for a given client application."""

    if not (user := Session.get(User, user_id)):
        raise x.ODPUserNotFound

    client_permissions = get_client_permissions(client_id)

    role_permissions = {}
    for role in user.roles:
        for scope in role.scopes:

            if ODPScope(scope.id).constrainable_by == 'collection' and role.collection_specific:
                role_permission = [collection.id for collection in role.collections]
            # elif ODPScope(scope.id).constrainable_by == 'provider':
            #     role_permission = []
            else:
                role_permission = '*'

            # we cannot grant a scope that is not available to the client
            if scope.id not in client_permissions:
                continue

            # add the scope granted by the role
            if (base_permission := role_permissions.get(scope.id)) is None:
                role_permissions[scope.id] = role_permission

            # the user already has the widest possible access to the scope
            elif base_permission == '*':
                pass

            # widen the user's access to the scope
            elif role_permission == '*':
                role_permissions[scope.id] = '*'

            # union the scope access granted by the user's roles
            else:
                role_permissions[scope.id] = list(set(base_permission) | set(role_permission))

    user_permissions = {}
    for scope_id, role_permission in role_permissions.items():

        # client allows any access to the scope; take that given by the roles
        if (client_permission := client_permissions[scope_id]) == '*':
            user_permissions[scope_id] = role_permission

        # roles allow any access to the scope; take that given by the client
        elif role_permission == '*':
            user_permissions[scope_id] = client_permission

        # intersect the scope access granted by the client and the roles
        # only grant the scope if the intersection is non-empty
        elif intersection := list(set(client_permission) & set(role_permission)):
            user_permissions[scope_id] = intersection

    return user_permissions


def get_user_info(user_id: str) -> UserInfo:
    """Return user profile info, which may be linked with a user's ID token."""

    if not (user := Session.get(User, user_id)):
        raise x.ODPUserNotFound

    return UserInfo(
        sub=user_id,
        email=user.email,
        email_verified=user.verified,
        name=user.name,
        picture=user.picture,
        roles=[role.id for role in user.roles],
    )
