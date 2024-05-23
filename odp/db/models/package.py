import uuid

from sqlalchemy import CheckConstraint, Column, Enum, ForeignKey, ForeignKeyConstraint, Identity, Integer, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.const.db import AuditCommand, PackageStatus, TagType
from odp.db import Base


class Package(Base):
    """A submission information package originating from a data provider."""

    __tablename__ = 'package'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    title = Column(String, nullable=False)
    status = Column(Enum(PackageStatus), nullable=False)
    notes = Column(String)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    provider_id = Column(String, ForeignKey('provider.id', ondelete='RESTRICT'), nullable=False)
    provider = relationship('Provider')

    # many-to-many package_resource entities are persisted by
    # assigning/removing Resource instances to/from resources
    package_resources = relationship('PackageResource', cascade='all, delete-orphan', passive_deletes=True)
    resources = association_proxy('package_resources', 'resource', creator=lambda r: PackageResource(resource=r))

    # view of associated tags (one-to-many)
    tags = relationship('PackageTag', viewonly=True)

    # view of associated record via one-to-many record_package relation
    # the plural 'records' is used because these attributes are collections,
    # although there can be only zero or one related record
    package_records = relationship('RecordPackage', viewonly=True)
    records = association_proxy('package_records', 'record')

    _repr_ = 'id', 'title', 'status', 'provider_id'


class PackageAudit(Base):
    """Package audit log."""

    __tablename__ = 'package_audit'

    id = Column(Integer, Identity(), primary_key=True)
    client_id = Column(String, nullable=False)
    user_id = Column(String)
    command = Column(Enum(AuditCommand), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _id = Column(String, nullable=False)
    _title = Column(String, nullable=False)
    _status = Column(String, nullable=False)
    _notes = Column(String)
    _provider_id = Column(String, nullable=False)


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


class PackageTag(Base):
    """Tag instance model, representing a tag attached to a package."""

    __tablename__ = 'package_tag'

    __table_args__ = (
        ForeignKeyConstraint(
            ('tag_id', 'tag_type'), ('tag.id', 'tag.type'),
            name='package_tag_tag_fkey', ondelete='CASCADE',
        ),
        CheckConstraint(
            f"tag_type = '{TagType.package}'",
            name='package_tag_tag_type_check',
        ),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    package_id = Column(String, ForeignKey('package.id', ondelete='CASCADE'), nullable=False)
    tag_id = Column(String, nullable=False)
    tag_type = Column(Enum(TagType), nullable=False)
    user_id = Column(String, ForeignKey('user.id', ondelete='RESTRICT'))

    data = Column(JSONB, nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    package = relationship('Package')
    tag = relationship('Tag')
    user = relationship('User')


class PackageTagAudit(Base):
    """Package tag audit log."""

    __tablename__ = 'package_tag_audit'

    id = Column(Integer, Identity(), primary_key=True)
    client_id = Column(String, nullable=False)
    user_id = Column(String)
    command = Column(Enum(AuditCommand), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _id = Column(String, nullable=False)
    _package_id = Column(String, nullable=False)
    _tag_id = Column(String, nullable=False)
    _user_id = Column(String)
    _data = Column(JSONB, nullable=False)
