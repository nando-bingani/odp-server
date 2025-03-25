from sqlalchemy import Column, Enum, String, TIMESTAMP

from odp.const.db import SchemaType
from odp.db import Base


class Schema(Base):
    """Represents a reference to a JSON schema document."""

    __tablename__ = 'schema'

    id = Column(String, unique=True, primary_key=True)
    type = Column(Enum(SchemaType), primary_key=True)
    uri = Column(String, nullable=False)
    md5 = Column(String, nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)
    template_uri = Column(String)

    _repr_ = 'id', 'type', 'uri'
