from sqlalchemy import Boolean, CheckConstraint, Column, Enum, ForeignKey, ForeignKeyConstraint, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.const.db import ScopeType
from odp.db import Base


class Role(Base):
    """A role is a configuration object that defines a set of
    permissions - represented by the associated scopes - that
    may be granted to a user.

    If a role is provider-specific, then any provider, package,
    resource, collection and record scopes apply only to that
    provider or to objects associated with that provider.

    If a role is collection-specific, then any collection and
    record scopes apply only to those collections or to objects
    associated with those collections.

    Note the ON DELETE CASCADE rule on provider_id, which
    ensures that provider deletion does not widen any access
    already granted by the role.
    """

    __tablename__ = 'role'

    id = Column(String, primary_key=True)

    provider_id = Column(String, ForeignKey('provider.id', ondelete='CASCADE'))
    provider = relationship('Provider')

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

    _repr_ = 'id', 'provider_id', 'collection_specific'


class RoleCollection(Base):
    """Model of a many-to-many role-collection association,
    representing the collections to which a role is restricted."""

    __tablename__ = 'role_collection'

    role_id = Column(String, ForeignKey('role.id', ondelete='CASCADE'), primary_key=True)
    collection_id = Column(String, ForeignKey('collection.id', ondelete='CASCADE'), primary_key=True)

    role = relationship('Role', viewonly=True)
    collection = relationship('Collection')

    _repr_ = 'role_id', 'collection_id'


class RoleScope(Base):
    """Model of a many-to-many role-scope association."""

    __tablename__ = 'role_scope'

    __table_args__ = (
        ForeignKeyConstraint(
            ('scope_id', 'scope_type'), ('scope.id', 'scope.type'),
            name='role_scope_scope_fkey', ondelete='CASCADE',
        ),
        CheckConstraint(
            f"scope_type in ('{ScopeType.odp}', '{ScopeType.client}')",
            name='role_scope_scope_type_check',
        ),
    )

    role_id = Column(String, ForeignKey('role.id', ondelete='CASCADE'), primary_key=True)
    scope_id = Column(String, primary_key=True)
    scope_type = Column(Enum(ScopeType), primary_key=True)

    role = relationship('Role', viewonly=True)
    scope = relationship('Scope')

    _repr_ = 'role_id', 'scope_id'
