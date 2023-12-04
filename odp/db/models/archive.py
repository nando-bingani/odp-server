from sqlalchemy import Column, String

from odp.db import Base


class Archive(Base):
    """An archive represents a data store that provides
    long-term preservation of and access to digital resources."""

    __tablename__ = 'archive'

    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)

    _repr_ = 'id', 'url'
