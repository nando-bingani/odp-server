from sqlalchemy import CheckConstraint, Column, Enum, ForeignKey, ForeignKeyConstraint, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.orm import relationship

from odp.const.db import ArchiveAdapter, ScopeType
from odp.db import Base


class Archive(Base):
    """A data store for digital resources.

    `url` is for downloads, `dir` for uploads.
    """

    __tablename__ = 'archive'

    __table_args__ = (
        ForeignKeyConstraint(
            ('scope_id', 'scope_type'), ('scope.id', 'scope.type'),
            name='archive_scope_fkey', ondelete='RESTRICT',
        ),
        CheckConstraint(
            f"scope_type = '{ScopeType.odp}'",
            name='archive_scope_type_check',
        ),
    )

    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    dir = Column(String)
    adapter = Column(Enum(ArchiveAdapter), nullable=False)

    scope_id = Column(String, nullable=False)
    scope_type = Column(Enum(ScopeType), nullable=False)
    scope = relationship('Scope')

    _repr_ = 'id', 'url', 'dir', 'adapter', 'scope_id'


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
