from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship

from odp.db import Base


class ClientCollection(Base):
    """Model of a many-to-many client-collection association,
    representing the collections to which a client is restricted."""

    __tablename__ = 'client_collection'

    client_id = Column(String, ForeignKey('client.id', ondelete='CASCADE'), primary_key=True)
    collection_id = Column(String, ForeignKey('collection.id', ondelete='CASCADE'), primary_key=True)

    client = relationship('Client', viewonly=True)
    collection = relationship('Collection')
