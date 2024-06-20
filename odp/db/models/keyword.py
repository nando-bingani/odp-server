from sqlalchemy import CheckConstraint, Column, Enum, ForeignKey, ForeignKeyConstraint, Identity, Integer, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from odp.const.db import AuditCommand, KeywordStatus, SchemaType
from odp.db import Base


class Keyword(Base):
    """A keyword identifies and describes a term in a vocabulary,
    which may be hierarchical.

    A keyword's key consists of its parent's key plus a non-empty
    suffix.

    A vocabulary is simply a keyword that is a parent of a set of
    keywords. One keyword in such a set may itself represent a
    vocabulary, with an associated set of sub-keywords.

    Every root vocabulary must have a child schema, which applies
    recursively to every keyword in its hierarchy. Any keyword
    may introduce its own child schema, which applies recursively
    to every sub-keyword in its sub-hierarchy.
    """

    __tablename__ = 'keyword'

    __table_args__ = (
        CheckConstraint(
            'parent_key is null or starts_with(key, parent_key)',
            name='keyword_parent_key_suffix_check',
        ),
        CheckConstraint(
            'parent_key is not null or child_schema_id is not null',
            name='keyword_root_child_schema_check',
        ),
        CheckConstraint(
            f"child_schema_type = '{SchemaType.keyword}'",
            name='keyword_child_schema_type_check',
        ),
        ForeignKeyConstraint(
            ('child_schema_id', 'child_schema_type'), ('schema.id', 'schema.type'),
            name='keyword_child_schema_fkey', ondelete='RESTRICT',
        ),
    )

    key = Column(String, primary_key=True)
    data = Column(JSONB, nullable=False)
    status = Column(Enum(KeywordStatus), nullable=False)

    parent_key = Column(String, ForeignKey('keyword.key', ondelete='RESTRICT'))
    parent = relationship('Keyword', remote_side=key)
    children = relationship('Keyword', order_by='Keyword.key', viewonly=True)

    child_schema_id = Column(String)
    child_schema_type = Column(Enum(SchemaType))
    child_schema = relationship('Schema')

    _repr_ = 'key', 'status', 'child_schema_id'


class KeywordAudit(Base):
    """Keyword audit log."""

    __tablename__ = 'keyword_audit'

    id = Column(Integer, Identity(), primary_key=True)
    client_id = Column(String, nullable=False)
    user_id = Column(String)
    command = Column(Enum(AuditCommand), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _key = Column(String, nullable=False)
    _data = Column(JSONB, nullable=False)
    _status = Column(String, nullable=False)
    _child_schema_id = Column(String)
