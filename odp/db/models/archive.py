import uuid

from sqlalchemy import BigInteger, Column, ForeignKey, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.orm import relationship

from odp.db import Base


class Archive(Base):
    """An archive represents a data store that provides
    long-term preservation of and access to digital resources."""

    __tablename__ = 'archive'

    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)

    _repr_ = 'id', 'url'


class ArchiveResource(Base):
    """A reference to a file or dataset in an archive.

    `key` is the archive's unique identifier for the resource.
    `path`, if specified, is relative to `url` of the archive.
    """

    __tablename__ = 'archive_resource'

    __table_args__ = (
        UniqueConstraint(
            'archive_id', 'key',
            name='uix_archive_resource_archive_id_key',
        ),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    key = Column(String, nullable=False)
    path = Column(String)
    name = Column(String)
    type = Column(String)
    size = Column(BigInteger)
    md5 = Column(String)
    timestamp = Column(TIMESTAMP(timezone=True))

    archive_id = Column(String, ForeignKey('archive.id', ondelete='CASCADE'), nullable=False)
    archive = relationship('Archive')

    _repr_ = 'id', 'key', 'path', 'name', 'type', 'size', 'archive_id'
