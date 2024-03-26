import uuid

from sqlalchemy import CheckConstraint, Column, Enum, ForeignKey, ForeignKeyConstraint, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.const.db import SchemaType
from odp.db import Base


class Package(Base):
    """A package represents a set of resources that constitute a
    digital object, and includes original metadata from the provider."""

    __tablename__ = 'package'

    __table_args__ = (
        ForeignKeyConstraint(
            ('schema_id', 'schema_type'), ('schema.id', 'schema.type'),
            name='package_schema_fkey', ondelete='RESTRICT',
        ),
        CheckConstraint(
            f"schema_type = '{SchemaType.metadata}'",
            name='package_schema_type_check',
        ),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    metadata_ = Column(JSONB, nullable=False)
    validity = Column(JSONB, nullable=False)
    notes = Column(String)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    provider_id = Column(String, ForeignKey('provider.id', ondelete='RESTRICT'), nullable=False)
    provider = relationship('Provider')

    schema_id = Column(String, nullable=False)
    schema_type = Column(Enum(SchemaType), nullable=False)
    schema = relationship('Schema')

    # many-to-many package_resource entities are persisted by
    # assigning/removing Resource instances to/from resources
    package_resources = relationship('PackageResource', cascade='all, delete-orphan', passive_deletes=True)
    resources = association_proxy('package_resources', 'resource', creator=lambda r: PackageResource(resource=r))

    _repr_ = 'id', 'provider_id', 'schema_id'


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
