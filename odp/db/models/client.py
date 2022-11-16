from sqlalchemy import Boolean, Column, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.db import Base
from odp.db.models.client_collection import ClientCollection
from odp.db.models.client_scope import ClientScope


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
