from sqlalchemy import Boolean, Column, Enum, ForeignKey, ForeignKeyConstraint, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.const.db import ScopeType
from odp.db import Base


class Client(Base):
    """A client application, linked by id with an OAuth2 client
    configuration on Hydra.

    The associated scopes represent the set of permissions granted
    to the client.

    If a client is collection-specific, then its scopes apply
    only to entities linked with the associated collections.
    """

    __tablename__ = 'client'

    id = Column(String, primary_key=True)
    collection_specific = Column(Boolean, nullable=False, server_default='false')

    # many-to-many client_scope entities are persisted by
    # assigning/removing Scope instances to/from scopes
    client_scopes = relationship('ClientScope', cascade='all, delete-orphan', passive_deletes=True)
    scopes = association_proxy('client_scopes', 'scope', creator=lambda s: ClientScope(scope=s))

    # many-to-many client_collection entities are persisted by
    # assigning/removing Collection instances to/from collections
    client_collections = relationship('ClientCollection', cascade='all, delete-orphan', passive_deletes=True)
    collections = association_proxy('client_collections', 'collection', creator=lambda c: ClientCollection(collection=c))

    _repr_ = 'id', 'collection_specific'


class ClientCollection(Base):
    """Model of a many-to-many client-collection association,
    representing the collections to which a client is restricted."""

    __tablename__ = 'client_collection'

    client_id = Column(String, ForeignKey('client.id', ondelete='CASCADE'), primary_key=True)
    collection_id = Column(String, ForeignKey('collection.id', ondelete='CASCADE'), primary_key=True)

    client = relationship('Client', viewonly=True)
    collection = relationship('Collection')

    _repr_ = 'client_id', 'collection_id'


class ClientScope(Base):
    """Model of a many-to-many client-scope association,
    representing the set of OAuth2 scopes that a client
    may request."""

    __tablename__ = 'client_scope'

    __table_args__ = (
        ForeignKeyConstraint(
            ('scope_id', 'scope_type'), ('scope.id', 'scope.type'),
            name='client_scope_scope_fkey', ondelete='CASCADE',
        ),
    )

    client_id = Column(String, ForeignKey('client.id', ondelete='CASCADE'), primary_key=True)
    scope_id = Column(String, primary_key=True)
    scope_type = Column(Enum(ScopeType), primary_key=True)

    client = relationship('Client', viewonly=True)
    scope = relationship('Scope')

    _repr_ = 'client_id', 'scope_id'
