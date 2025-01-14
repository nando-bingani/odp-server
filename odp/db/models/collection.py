import uuid

from sqlalchemy import CheckConstraint, Column, Enum, ForeignKey, ForeignKeyConstraint, Identity, Integer, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.const.db import AuditCommand, TagType
from odp.db import Base


class Collection(Base):
    """A collection of ODP records."""

    __tablename__ = 'collection'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    key = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    doi_key = Column(String)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    provider_id = Column(String, ForeignKey('provider.id', ondelete='CASCADE'), nullable=False)
    provider = relationship('Provider')

    # view of associated tags (one-to-many)
    tags = relationship('CollectionTag', viewonly=True)

    # view of associated roles via many-to-many role_collection relation
    collection_roles = relationship('RoleCollection', viewonly=True)
    roles = association_proxy('collection_roles', 'role')

    _repr_ = 'id', 'key', 'name', 'doi_key', 'provider_id'


class CollectionAudit(Base):
    """Collection audit log."""

    __tablename__ = 'collection_audit'

    id = Column(Integer, Identity(), primary_key=True)
    client_id = Column(String, nullable=False)
    user_id = Column(String)
    command = Column(Enum(AuditCommand), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _id = Column(String, nullable=False)
    _key = Column(String, nullable=False)
    _name = Column(String, nullable=False)
    _doi_key = Column(String)
    _provider_id = Column(String, nullable=False)


class CollectionTag(Base):
    """Tag instance model, representing a tag attached to a collection."""

    __tablename__ = 'collection_tag'

    __table_args__ = (
        ForeignKeyConstraint(
            ('tag_id', 'tag_type'), ('tag.id', 'tag.type'),
            name='collection_tag_tag_fkey', ondelete='CASCADE',
        ),
        CheckConstraint(
            f"tag_type = '{TagType.collection}'",
            name='collection_tag_tag_type_check',
        ),
        ForeignKeyConstraint(
            ('vocabulary_id', 'keyword_id'), ('keyword.vocabulary_id', 'keyword.id'),
            name='collection_tag_keyword_fkey', ondelete='RESTRICT',
        ),
    )

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    collection_id = Column(String, ForeignKey('collection.id', ondelete='CASCADE'), nullable=False)
    tag_id = Column(String, nullable=False)
    tag_type = Column(Enum(TagType), nullable=False)
    user_id = Column(String, ForeignKey('user.id', ondelete='RESTRICT'))

    vocabulary_id = Column(String)
    keyword_id = Column(Integer)

    data = Column(JSONB, nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    collection = relationship('Collection')
    tag = relationship('Tag')
    user = relationship('User')
    keyword = relationship('Keyword')


class CollectionTagAudit(Base):
    """Collection tag audit log."""

    __tablename__ = 'collection_tag_audit'

    id = Column(Integer, Identity(), primary_key=True)
    client_id = Column(String, nullable=False)
    user_id = Column(String)
    command = Column(Enum(AuditCommand), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _id = Column(String, nullable=False)
    _collection_id = Column(String, nullable=False)
    _tag_id = Column(String, nullable=False)
    _user_id = Column(String)
    _data = Column(JSONB, nullable=False)
    _keyword_id = Column(Integer)
