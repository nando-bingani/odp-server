from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship

from odp.db import Base


class CatalogCollection(Base):
    """Represents the set of published collections for a catalog."""

    __tablename__ = 'catalog_collection'

    catalog_id = Column(String, ForeignKey('catalog.id', ondelete='CASCADE'), primary_key=True)
    collection_id = Column(String, ForeignKey('collection.id', ondelete='CASCADE'), primary_key=True)

    catalog = relationship('Catalog', viewonly=True)
    collection = relationship('Collection', viewonly=True)
