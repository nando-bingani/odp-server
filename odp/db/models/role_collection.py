from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship

from odp.db import Base


class RoleCollection(Base):
    """Model of a many-to-many role-collection association,
    representing the collections to which a role is restricted."""

    __tablename__ = 'role_collection'

    role_id = Column(String, ForeignKey('role.id', ondelete='CASCADE'), primary_key=True)
    collection_id = Column(String, ForeignKey('collection.id', ondelete='CASCADE'), primary_key=True)

    role = relationship('Role', viewonly=True)
    collection = relationship('Collection')
