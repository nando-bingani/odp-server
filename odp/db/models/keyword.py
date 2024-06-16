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

    A vocabulary is a keyword that is a parent of a set of keywords.
    One keyword in such a set may itself represent a vocabulary,
    with an associated set of sub-keywords.

    Every root vocabulary must have a schema, which applies
    recursively to every keyword in its hierarchy. Any keyword
    may introduce its own schema, which applies to itself and
    recursively to every sub-keyword in its sub-hierarchy.
    """

    __tablename__ = 'keyword'

    __table_args__ = (
        ForeignKeyConstraint(
            ('schema_id', 'schema_type'), ('schema.id', 'schema.type'),
            name='keyword_schema_fkey', ondelete='RESTRICT',
        ),
        CheckConstraint(
            f"schema_type = '{SchemaType.keyword}'",
            name='keyword_schema_type_check',
        ),
        CheckConstraint(
            'parent_key is not null or schema_id is not null',
            name='keyword_parent_schema_check',
        ),
        CheckConstraint(
            'parent_key is null or starts_with(key, parent_key)',
            name='keyword_parent_suffix_check',
        ),
    )

    parent_key = Column(String, ForeignKey('keyword.key', ondelete='RESTRICT'))
    key = Column(String, primary_key=True)
    data = Column(JSONB, nullable=False)
    status = Column(Enum(KeywordStatus), nullable=False)

    schema_id = Column(String)
    schema_type = Column(Enum(SchemaType))
    schema = relationship('Schema')

    parent = relationship('Keyword', remote_side=key)
    children = relationship('Keyword', viewonly=True)

    _repr_ = 'parent_key', 'key', 'status', 'schema_id'


class KeywordAudit(Base):
    """Keyword audit log."""

    __tablename__ = 'keyword_audit'

    id = Column(Integer, Identity(), primary_key=True)
    client_id = Column(String, nullable=False)
    user_id = Column(String)
    command = Column(Enum(AuditCommand), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _parent_key = Column(String)
    _key = Column(String, nullable=False)
    _data = Column(JSONB, nullable=False)
    _status = Column(String, nullable=False)
    _schema_id = Column(String)
