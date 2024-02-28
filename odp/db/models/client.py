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

    If a client is provider-specific, then any package and resource
    scopes apply only to objects associated with the referenced
    provider.

    The `provider_specific` attribute ensures that provider deletion
    does not widen any package/resource access granted to the client.
    This is preferable to a delete cascade rule on `provider_id`,
    since we need to delete a client via the API to ensure that
    configuration is removed from Hydra.
    """

    __tablename__ = 'client'

    id = Column(String, primary_key=True)

    provider_specific = Column(Boolean, nullable=False, server_default='false')
    provider_id = Column(String, ForeignKey('provider.id', ondelete='SET NULL'))
    provider = relationship('Provider')

    # many-to-many client_scope entities are persisted by
    # assigning/removing Scope instances to/from scopes
    client_scopes = relationship('ClientScope', cascade='all, delete-orphan', passive_deletes=True)
    scopes = association_proxy('client_scopes', 'scope', creator=lambda s: ClientScope(scope=s))

    _repr_ = 'id', 'provider_specific', 'provider_id'


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
