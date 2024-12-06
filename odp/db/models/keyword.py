from sqlalchemy import Column, Enum, ForeignKey, Identity, Integer, String, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from odp.const.db import AuditCommand, KeywordStatus
from odp.db import Base


class Keyword(Base):
    """A keyword identifies and describes a term in a vocabulary,
    which may be hierarchical."""

    __tablename__ = 'keyword'

    __table_args__ = (
        UniqueConstraint('vocabulary_id', 'key'),
    )

    vocabulary_id = Column(String, ForeignKey('vocabulary.id', ondelete='CASCADE'), nullable=False)
    vocabulary = relationship('Vocabulary')

    id = Column(Integer, Identity(start=1001), primary_key=True)
    key = Column(String, nullable=False)
    data = Column(JSONB, nullable=False)
    status = Column(Enum(KeywordStatus), nullable=False)

    parent_id = Column(Integer, ForeignKey('keyword.id', ondelete='RESTRICT'))
    parent = relationship('Keyword', remote_side=id)
    children = relationship('Keyword', order_by='Keyword.id', viewonly=True)

    _repr_ = 'vocabulary_id', 'id', 'key', 'status', 'parent_id'


class KeywordAudit(Base):
    """Keyword audit log."""

    __tablename__ = 'keyword_audit'

    id = Column(Integer, Identity(), primary_key=True)
    client_id = Column(String, nullable=False)
    user_id = Column(String)
    command = Column(Enum(AuditCommand), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _vocabulary_id = Column(String, nullable=False)
    _id = Column(Integer, nullable=False)
    _key = Column(String, nullable=False)
    _data = Column(JSONB, nullable=False)
    _status = Column(String, nullable=False)
    _parent_id = Column(Integer)
