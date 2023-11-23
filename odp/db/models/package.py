import uuid

from sqlalchemy import Column, ForeignKey, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from odp.db import Base


class Package(Base):
    """A package represents a set of resources that constitute
    a digital object.

    A package may include non-authoritative metadata, which can
    be used when creating/updating the linked record.
    """

    __tablename__ = 'package'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    metadata_ = Column(JSONB)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    provider_id = Column(String, ForeignKey('provider.id', ondelete='CASCADE'), nullable=False)
    provider = relationship('Provider')

    record_id = Column(String, ForeignKey('record.id', ondelete='SET NULL'))
    record = relationship('Record')

    _repr_ = 'id', 'provider_id', 'record_id',


class PackageResource(Base):
    """Model of a many-to-many package-resource association,
    representing the set of archived resources constituting a package."""

    __tablename__ = 'package_resource'

    package_id = Column(String, ForeignKey('package.id', ondelete='CASCADE'), primary_key=True)
    resource_id = Column(String, ForeignKey('archive_resource.id', ondelete='RESTRICT'), primary_key=True)

    package = relationship('Package', viewonly=True)
    resource = relationship('ArchiveResource', viewonly=True)
