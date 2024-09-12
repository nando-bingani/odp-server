import uuid

from sqlalchemy import BigInteger, CheckConstraint, Column, Enum, ForeignKey, String, TIMESTAMP
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.const.db import HashAlgorithm
from odp.db import Base


class Resource(Base):
    """A resource comprises the metadata for an individual file, folder or dataset."""

    __tablename__ = 'resource'

    __table_args__ = (
        CheckConstraint(
            'hash is null or hash_algorithm is not null',
            name='resource_hash_algorithm_check',
        ),
        CheckConstraint(
            'title is not null or filename is not null',
            name='resource_title_or_filename_check',
        ),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    title = Column(String)
    description = Column(String)
    filename = Column(String)
    mimetype = Column(String)
    size = Column(BigInteger)
    hash = Column(String)
    hash_algorithm = Column(Enum(HashAlgorithm))
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    provider_id = Column(String, ForeignKey('provider.id', ondelete='RESTRICT'), nullable=False)
    provider = relationship('Provider')

    # view of associated packages via many-to-many package_resource relation
    resource_packages = relationship('PackageResource', viewonly=True)
    packages = association_proxy('resource_packages', 'package')

    # view of associated archives via many-to-many archive_resource relation
    archive_resources = relationship('ArchiveResource', viewonly=True)
    archives = association_proxy('archive_resources', 'archive')

    _repr_ = 'id', 'title', 'filename', 'mimetype', 'size', 'md5', 'provider_id'
