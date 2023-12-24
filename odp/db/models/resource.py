import uuid

from sqlalchemy import BigInteger, CheckConstraint, Column, Enum, ForeignKey, String, TIMESTAMP, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.db import Base
from odp.db.models.types import ResourceType


class Resource(Base):
    """A reference to a file, folder or web page in an archive.

    `path` is relative to `url` of the archive.

    `text_data` and `binary_data` may be used to store data
    temporarily, pending archival.

    Note the preclusion of foreign key delete cascades:
    If the containing archive or the resource provider must be deleted,
    then the resource must either be moved or explicitly deleted first.
    """

    __tablename__ = 'resource'

    __table_args__ = (
        UniqueConstraint(
            'archive_id', 'path'
        ),
        CheckConstraint(
            f"type = '{ResourceType.file}' OR (text_data IS NULL AND binary_data IS NULL)",
            name='resource_type_data_check',
        ),
        CheckConstraint(
            "text_data IS NULL OR binary_data IS NULL",
            name='resource_text_binary_check',
        ),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    path = Column(String, nullable=False)
    type = Column(Enum(ResourceType), nullable=False)

    # file metadata
    name = Column(String)
    size = Column(BigInteger)
    md5 = Column(String)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    # file cache
    text_data = Column(Text)
    binary_data = Column(BYTEA)

    provider_id = Column(String, ForeignKey('provider.id', ondelete='RESTRICT'), nullable=False)
    provider = relationship('Provider')

    archive_id = Column(String, ForeignKey('archive.id', ondelete='RESTRICT'), nullable=False)
    archive = relationship('Archive')

    # view of associated packages via many-to-many package_resource relation
    resource_packages = relationship('PackageResource', viewonly=True)
    packages = association_proxy('resource_packages', 'package')

    _repr_ = 'id', 'path', 'type', 'name', 'size', 'archive_id', 'provider_id'
