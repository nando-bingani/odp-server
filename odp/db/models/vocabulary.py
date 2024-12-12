from sqlalchemy import Boolean, CheckConstraint, Column, Enum, ForeignKeyConstraint, String
from sqlalchemy.orm import relationship

from odp.const.db import SchemaType, ScopeType
from odp.db import Base


class Vocabulary(Base):
    """A vocabulary is a collection of keywords, each of which is
    structured according to the vocabulary's schema."""

    __tablename__ = 'vocabulary'

    __table_args__ = (
        ForeignKeyConstraint(
            ('schema_id', 'schema_type'), ('schema.id', 'schema.type'),
            name='vocabulary_schema_fkey', ondelete='RESTRICT',
        ),
        CheckConstraint(
            # todo: schema type 'vocabulary' is deprecated; clean up check once removed
            f"schema_type in ('{SchemaType.keyword}', '{SchemaType.vocabulary}')",
            name='vocabulary_schema_type_check',
        ),
        ForeignKeyConstraint(
            ('scope_id', 'scope_type'), ('scope.id', 'scope.type'),
            name='vocabulary_scope_fkey', ondelete='RESTRICT',
        ),
        CheckConstraint(
            f"scope_type = '{ScopeType.odp}'",
            name='vocabulary_scope_type_check',
        ),
    )

    id = Column(String, primary_key=True)
    uri = Column(String)

    schema_id = Column(String, nullable=False)
    schema_type = Column(Enum(SchemaType), nullable=False)
    schema = relationship('Schema')

    scope_id = Column(String, nullable=False)
    scope_type = Column(Enum(ScopeType), nullable=False)
    scope = relationship('Scope')

    # if static, keywords are maintained by the system
    static = Column(Boolean, nullable=False, server_default='false')

    # view of associated keywords (one-to-many)
    keywords = relationship('Keyword', viewonly=True)

    _repr_ = 'id', 'schema_id', 'scope_id', 'static'
