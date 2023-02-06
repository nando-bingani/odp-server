from sqlalchemy import ARRAY, Boolean, Column, Enum, Identity, Integer, String, TIMESTAMP

from odp.db import Base
from odp.db.models.types import IdentityCommand


class IdentityAudit(Base):
    """User identity audit log."""

    __tablename__ = 'identity_audit'

    id = Column(Integer, Identity(), primary_key=True)
    client_id = Column(String, nullable=False)
    user_id = Column(String)
    command = Column(Enum(IdentityCommand), nullable=False)
    completed = Column(Boolean, nullable=False)
    error = Column(String)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    _id = Column(String)
    _email = Column(String)
    _active = Column(String)
    _roles = Column(ARRAY(String))
