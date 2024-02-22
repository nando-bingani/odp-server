import uuid

from sqlalchemy import Column, ForeignKey, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.db import Base


class Package(Base):
    """A package represents a set of resources that constitute
    a digital object.

    A package may include non-authoritative metadata.
    """

    __tablename__ = 'package'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    metadata_ = Column(JSONB)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    provider_id = Column(String, ForeignKey('provider.id', ondelete='CASCADE'), nullable=False)
    provider = relationship('Provider')

    # many-to-many package_resource entities are persisted by
    # assigning/removing Resource instances to/from resources
    package_resources = relationship('PackageResource', cascade='all, delete-orphan', passive_deletes=True)
    resources = association_proxy('package_resources', 'resource', creator=lambda r: PackageResource(resource=r))

    _repr_ = 'id', 'provider_id'


class PackageResource(Base):
    """Model of a many-to-many package-resource association,
    representing the set of resources constituting a package.

    A resource cannot be deleted if it is part of a package.
    """

    __tablename__ = 'package_resource'

    package_id = Column(String, ForeignKey('package.id', ondelete='CASCADE'), primary_key=True)
    resource_id = Column(String, ForeignKey('resource.id', ondelete='RESTRICT'), primary_key=True)

    package = relationship('Package', viewonly=True)
    resource = relationship('Resource')
