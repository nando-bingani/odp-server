from sqlalchemy import Boolean, Column, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.db import Base
from odp.db.models.role_collection import RoleCollection
from odp.db.models.role_scope import RoleScope


class Role(Base):
    """A role is a configuration object that defines a set of
    permissions - represented by the associated scopes - that
    may be granted to a user.

    If a role is collection-specific, then its scopes apply
    only to entities linked with the associated collections.
    """

    __tablename__ = 'role'

    id = Column(String, primary_key=True)
    collection_specific = Column(Boolean, nullable=False, server_default='false')

    # many-to-many role_scope entities are persisted by
    # assigning/removing Scope instances to/from scopes
    role_scopes = relationship('RoleScope', cascade='all, delete-orphan', passive_deletes=True)
    scopes = association_proxy('role_scopes', 'scope', creator=lambda s: RoleScope(scope=s))

    # many-to-many role_collection entities are persisted by
    # assigning/removing Collection instances to/from collections
    role_collections = relationship('RoleCollection', cascade='all, delete-orphan', passive_deletes=True)
    collections = association_proxy('role_collections', 'collection', creator=lambda c: RoleCollection(collection=c))

    # view of associated users via many-to-many user_role relation
    role_users = relationship('UserRole', viewonly=True)
    users = association_proxy('role_users', 'user')

    _repr_ = 'id', 'collection_specific'
