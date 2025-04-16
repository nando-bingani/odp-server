from sqlalchemy import CheckConstraint, Column, Enum, ForeignKey, ForeignKeyConstraint, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.orm import relationship

from odp.const.db import ArchiveResourceStatus, ArchiveType, ScopeType
from odp.db import Base


class Archive(Base):
    """A data store for digital resources."""

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
    type = Column(Enum(ArchiveType), nullable=False)
    download_url = Column(String)
    upload_url = Column(String)

    scope_id = Column(String, nullable=False)
    scope_type = Column(Enum(ScopeType), nullable=False)
    scope = relationship('Scope')

    _repr_ = 'id', 'type', 'download_url', 'upload_url', 'scope_id'


class ArchiveResource(Base):
    """An archived instance of a resource.

    `path` is relative to the archive's upload and download URLs.
    """

    __tablename__ = 'archive_resource'

    __table_args__ = (
        UniqueConstraint('archive_id', 'path'),
    )

    archive_id = Column(String, ForeignKey('archive.id', ondelete='RESTRICT'), primary_key=True)
    resource_id = Column(String, ForeignKey('resource.id', ondelete='RESTRICT'), primary_key=True)

    archive = relationship('Archive')
    resource = relationship('Resource')

    path = Column(String, nullable=False)
    status = Column(Enum(ArchiveResourceStatus), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _repr_ = 'archive_id', 'resource_id', 'path', 'status'
