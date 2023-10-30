from sqlalchemy import ARRAY, Boolean, Column, ForeignKey, ForeignKeyConstraint, Identity, Index, Integer, Numeric, String, TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from sqlalchemy.orm import deferred, relationship

from odp.db import Base


class Catalog(Base):
    """Represents a public catalog providing access to published
    digital object records."""

    __tablename__ = 'catalog'

    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)

    # global data published for the catalog
    data = Column(JSONB)
    timestamp = Column(TIMESTAMP(timezone=True))

    _repr_ = 'id', 'url'


class CatalogRecord(Base):
    """Model of a many-to-many catalog-record association,
    representing the state of a record with respect to a
    public catalog."""

    __tablename__ = 'catalog_record'

    __table_args__ = (
        Index('ix_catalog_record_catalog_id_timestamp', 'catalog_id', 'timestamp'),
        Index('ix_catalog_record_catalog_id_published_searchable', 'catalog_id', 'published', 'searchable'),
        Index('ix_catalog_record_full_text', 'full_text', postgresql_using='gin'),
        Index('ix_catalog_record_spatial', 'spatial_north', 'spatial_east', 'spatial_south', 'spatial_west'),
    )

    catalog_id = Column(String, ForeignKey('catalog.id', ondelete='CASCADE'), primary_key=True)
    record_id = Column(String, ForeignKey('record.id', ondelete='CASCADE'), primary_key=True)

    catalog = relationship('Catalog', viewonly=True)
    record = relationship('Record', viewonly=True)

    published = Column(Boolean, nullable=False)
    published_record = Column(JSONB)
    reason = Column(String)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)

    # external catalog integration
    synced = Column(Boolean)
    error = Column(String)
    error_count = Column(Integer)

    # internal catalog indexing
    full_text = deferred(Column(TSVECTOR))
    keywords = Column(ARRAY(String))
    facets = relationship('CatalogRecordFacet', cascade='all, delete-orphan', passive_deletes=True)
    spatial_north = Column(Numeric)
    spatial_east = Column(Numeric)
    spatial_south = Column(Numeric)
    spatial_west = Column(Numeric)
    temporal_start = Column(TIMESTAMP(timezone=True), index=True)
    temporal_end = Column(TIMESTAMP(timezone=True), index=True)
    searchable = Column(Boolean)


class CatalogRecordFacet(Base):
    __tablename__ = 'catalog_record_facet'

    __table_args__ = (
        ForeignKeyConstraint(
            ('catalog_id', 'record_id'),
            ('catalog_record.catalog_id', 'catalog_record.record_id'),
            name='catalog_record_facet_catalog_record_fkey', ondelete='CASCADE',
        ),
        Index('ix_catalog_record_facet_catalog_id_facet_value', 'catalog_id', 'facet', 'value'),
    )

    id = Column(Integer, Identity(), primary_key=True)
    catalog_id = Column(String, nullable=False)
    record_id = Column(String, nullable=False, index=True)
    facet = Column(String, nullable=False)
    value = Column(String, nullable=False)
