import uuid

from sqlalchemy import BigInteger, Column, ForeignKey, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.db import Base


class Resource(Base):
    """A description of a digital resource, which may be composed
    into a package constituting a digital object."""

    __tablename__ = 'resource'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    size = Column(BigInteger)
    md5 = Column(String)
    metadata_ = Column(JSONB)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    provider_id = Column(String, ForeignKey('provider.id', ondelete='RESTRICT'), nullable=False)
    provider = relationship('Provider')

    # view of associated packages via many-to-many package_resource relation
    resource_packages = relationship('PackageResource', viewonly=True)
    packages = association_proxy('resource_packages', 'package')

    # view of associated archives via many-to-many archive_resource relation
    archive_resources = relationship('ArchiveResource', viewonly=True)
    archives = association_proxy('archive_resources', 'archive')

    _repr_ = 'id', 'name', 'size', 'provider_id'
