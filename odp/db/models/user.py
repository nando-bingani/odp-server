import uuid
from datetime import datetime, timezone

from sqlalchemy import ARRAY, Boolean, Column, Enum, ForeignKey, Identity, Integer, String, TIMESTAMP
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship

from odp.const.db import IdentityCommand
from odp.db import Base


class User(Base):
    """A user account."""

    __tablename__ = 'user'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, nullable=False)
    password = Column(String)
    active = Column(Boolean, nullable=False)
    verified = Column(Boolean, nullable=False)
    name = Column(String, nullable=False)
    picture = Column(String)

    # many-to-many user_role entities are persisted by
    # assigning/removing Role instances to/from roles
    user_roles = relationship('UserRole', cascade='all, delete-orphan', passive_deletes=True)
    roles = association_proxy('user_roles', 'role', creator=lambda r: UserRole(role=r))

    # view of associated providers via many-to-many provider_user relation
    user_providers = relationship('ProviderUser', viewonly=True)
    providers = association_proxy('user_providers', 'provider')

    _repr_ = 'id', 'email', 'name', 'active', 'verified'


class UserRole(Base):
    """A user-role assignment."""

    __tablename__ = 'user_role'

    user_id = Column(String, ForeignKey('user.id', ondelete='CASCADE'), primary_key=True)
    role_id = Column(String, ForeignKey('role.id', ondelete='CASCADE'), primary_key=True)

    user = relationship('User', viewonly=True)
    role = relationship('Role')

    _repr_ = 'user_id', 'role_id'


class IdentityAudit(Base):
    """User identity audit log."""

    __tablename__ = 'identity_audit'

    id = Column(Integer, Identity(), primary_key=True)
    client_id = Column(String, nullable=False)
    user_id = Column(String)  # admin user id, for user edit/delete
    command = Column(Enum(IdentityCommand), nullable=False)
    completed = Column(Boolean, nullable=False)
    error = Column(String)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    _id = Column(String)
    _email = Column(String)
    _active = Column(Boolean)
    _roles = Column(ARRAY(String))
