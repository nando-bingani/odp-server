import uuid

from sqlalchemy import BigInteger, Column, Enum, ForeignKey, String, TIMESTAMP
from sqlalchemy.orm import relationship

from odp.db import Base
from odp.db.models.types import ResourceType


class Resource(Base):
    """A reference to a file, folder or web page in an archive.

    `path` is relative to `url` of the archive.

    Note the preclusion of foreign key delete cascades:
    If the containing archive or the resource provider must be deleted,
    then the resource must either be moved or explicitly deleted first.
    """

    __tablename__ = 'resource'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    type = Column(Enum(ResourceType), nullable=False)
    path = Column(String, nullable=False)
    name = Column(String)
    size = Column(BigInteger)
    md5 = Column(String)
    timestamp = Column(TIMESTAMP(timezone=True))

    archive_id = Column(String, ForeignKey('archive.id', ondelete='RESTRICT'), nullable=False)
    archive = relationship('Archive')

    provider_id = Column(String, ForeignKey('provider.id', ondelete='RESTRICT'), nullable=False)
    provider = relationship('Provider')

    _repr_ = 'id', 'path', 'name', 'type', 'size', 'archive_id', 'provider_id'
