import uuid

from sqlalchemy import ARRAY, CheckConstraint, Column, Enum, ForeignKey, ForeignKeyConstraint, Identity, Integer, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.const.db import AuditCommand, PackageCommand, PackageStatus, SchemaType, TagType
from odp.db import Base


class Package(Base):
    """A submission information package. A collection of resources
    and associated metadata originating from a data provider.

    The package `key` is unique to the provider.

    All package metadata - besides the title - are supplied via tags.
    """

    __tablename__ = 'package'

    __table_args__ = (
        UniqueConstraint('provider_id', 'key'),
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
    key = Column(String, nullable=False)
    title = Column(String, nullable=False)
    status = Column(Enum(PackageStatus), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    provider_id = Column(String, ForeignKey('provider.id', ondelete='RESTRICT'), nullable=False)
    provider = relationship('Provider')

    # view of associated resources (one-to-many)
    resources = relationship('Resource', viewonly=True)

    # view of associated tags (one-to-many)
    tags = relationship('PackageTag', viewonly=True)

    metadata_ = Column(JSONB)
    validity = Column(JSONB)
    schema_id = Column(String, nullable=False)
    schema_type = Column(Enum(SchemaType), nullable=False)
    schema = relationship('Schema')

    # view of associated record via one-to-many record_package relation
    # the plural 'records' is used because these attributes are collections,
    # although there can be only zero or one related record
    package_records = relationship('RecordPackage', viewonly=True)
    records = association_proxy('package_records', 'record')

    _repr_ = 'id', 'key', 'title', 'status', 'provider_id', 'schema_id'


class PackageAudit(Base):
    """Package audit log."""

    __tablename__ = 'package_audit'

    id = Column(Integer, Identity(), primary_key=True)
    client_id = Column(String, nullable=False)
    user_id = Column(String)
    command = Column(Enum(PackageCommand), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _id = Column(String, nullable=False)
    _key = Column(String, nullable=False)
    _title = Column(String, nullable=False)
    _status = Column(String, nullable=False)
    _provider_id = Column(String, nullable=False)
    _schema_id = Column(String, nullable=False)
    _resources = Column(ARRAY(String))


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
        ForeignKeyConstraint(
            ('vocabulary_id', 'keyword_id'), ('keyword.vocabulary_id', 'keyword.id'),
            name='package_tag_keyword_fkey', ondelete='RESTRICT',
        ),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    package_id = Column(String, ForeignKey('package.id', ondelete='CASCADE'), nullable=False)
    tag_id = Column(String, nullable=False)
    tag_type = Column(Enum(TagType), nullable=False)
    user_id = Column(String, ForeignKey('user.id', ondelete='RESTRICT'))

    vocabulary_id = Column(String)
    keyword_id = Column(Integer)

    data = Column(JSONB, nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    package = relationship('Package')
    tag = relationship('Tag')
    user = relationship('User')
    keyword = relationship('Keyword')


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
    _keyword_id = Column(Integer)
