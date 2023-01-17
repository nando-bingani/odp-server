from dataclasses import dataclass
from typing import Dict, List, Literal, Optional

from odp.db import Session
from odp.db.models import Client, User
from odp.lib import exceptions as x

Permissions = Dict[str, Literal['*'] | List[str]]
"""The effective set of permissions for a user or a client. A dictionary of
scope ids (OAuth2 scope identifiers), where the value for each id is either:

- '*' if the scope is applicable across all relevant platform entities; or
- a set of collection ids to which the scope's usage is limited (implemented
  as a list for JSON serialization)
"""


@dataclass
class UserInfo:
    sub: str
    email: str
    email_verified: bool
    name: Optional[str]
    picture: Optional[str]
    roles: List[str]


def get_client_permissions(client_id: str) -> Permissions:
    """Return effective client permissions."""
    client = Session.get(Client, client_id)
    if not client:
        raise x.ODPClientNotFound

    scope_applicability = ('*' if not client.collection_specific
                           else [collection.id for collection in client.collections])
    return {
        scope.id: scope_applicability
        for scope in client.scopes
    }


def get_user_permissions(user_id: str, client_id: str) -> Permissions:
    """Return effective user permissions, which may be linked with
    a user's access token for a given client application."""
    user = Session.get(User, user_id)
    if not user:
        raise x.ODPUserNotFound

    client = Session.get(Client, client_id)
    if not client:
        raise x.ODPClientNotFound

    platform_scopes = set()
    if not client.collection_specific:
        for role in user.roles:
            if not role.collection_specific:
                platform_scopes |= {
                    scope.id for scope in role.scopes
                    if scope in client.scopes
                }

    collection_scopes = {}
    for role in user.roles:
        if role.collection_specific and client.collection_specific:
            collection_ids = set(c.id for c in role.collections).intersection(c.id for c in client.collections)
        elif role.collection_specific:
            collection_ids = set(c.id for c in role.collections)
        elif client.collection_specific:
            collection_ids = set(c.id for c in client.collections)
        else:
            continue

        if not collection_ids:
            continue

        for scope in role.scopes:
            if scope.id in platform_scopes:
                continue
            if scope not in client.scopes:
                continue

            collection_scopes.setdefault(scope.id, set())
            collection_scopes[scope.id] |= collection_ids

    collection_scopes = {
        scope: list(collections)
        for scope, collections in collection_scopes.items()
    }

    return {scope: '*' for scope in platform_scopes} | collection_scopes


def get_user_info(user_id: str, client_id: str) -> UserInfo:
    """Return user profile info, which may be linked with a user's
    ID token for a given client application.

    TODO: we should limit the returned info based on the claims
     allowed for the client
    """
    user = Session.get(User, user_id)
    if not user:
        raise x.ODPUserNotFound

    client = Session.get(Client, client_id)
    if not client:
        raise x.ODPClientNotFound

    return UserInfo(
        sub=user_id,
        email=user.email,
        email_verified=user.verified,
        name=user.name,
        picture=user.picture,
        roles=[
            role.id for role in user.roles
            if (not role.collection_specific or not client.collection_specific
                or set(c.id for c in role.collections).intersection(c.id for c in client.collections))
        ],
    )
