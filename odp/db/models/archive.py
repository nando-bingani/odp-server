from sqlalchemy import Column, ForeignKey, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.orm import relationship

from odp.db import Base


class Archive(Base):
    """A data store for digital resources."""

    __tablename__ = 'archive'

    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)

    _repr_ = 'id', 'url'


class ArchiveResource(Base):
    """An archived instance of a resource.

    `path` is relative to `url` of the archive.
    """

    __tablename__ = 'archive_resource'

    __table_args__ = (
        UniqueConstraint('archive_id', 'path'),
    )

    archive_id = Column(String, ForeignKey('archive.id', ondelete='RESTRICT'), primary_key=True)
    resource_id = Column(String, ForeignKey('resource.id', ondelete='CASCADE'), primary_key=True)

    archive = relationship('Archive')
    resource = relationship('Resource')

    path = Column(String, nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _repr_ = 'archive_id', 'resource_id', 'path'
