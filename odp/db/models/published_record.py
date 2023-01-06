from datetime import datetime, timezone

from sqlalchemy import Column, ForeignKey, String, TIMESTAMP

from odp.db import Base


def _doi_published_timestamp(context):
    if context.get_current_parameters()['doi'] is not None:
        return datetime.now(timezone.utc)


class PublishedRecord(Base):
    """This table preserves all record ids and DOIs that have ever
    been published, and prevents associated records from being deleted
    or having their DOIs changed or removed."""

    __tablename__ = 'published_record'

    id = Column(String, ForeignKey('record.id', ondelete='RESTRICT', onupdate='RESTRICT'), primary_key=True)
    doi = Column(String, ForeignKey('record.doi', ondelete='RESTRICT', onupdate='RESTRICT'), unique=True)
    id_published = Column(TIMESTAMP(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    doi_published = Column(TIMESTAMP(timezone=True), default=_doi_published_timestamp, onupdate=_doi_published_timestamp)
