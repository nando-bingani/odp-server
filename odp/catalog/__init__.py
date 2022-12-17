import logging
from datetime import date, datetime
from enum import Enum
from typing import Optional, final

from sqlalchemy import func, or_, select

from odp.api.lib.utils import output_published_record_model
from odp.api.models import PublishedRecordModel, RecordModel
from odp.api.routers.record import output_record_model
from odp.cache import Cache
from odp.const import ODPCatalog, ODPCollectionTag, ODPMetadataSchema, ODPRecordTag
from odp.db import Session
from odp.db.models import CatalogRecord, Collection, Provider, PublishedDOI, Record, RecordTag

logger = logging.getLogger(__name__)


class PublishedReason(str, Enum):
    QC_PASSED = 'QC passed'
    COLLECTION_READY = 'collection ready'
    MIGRATED_PUBLISHED = 'migrated as published'
    MIMS_COLLECTION = 'MIMS collection'


class NotPublishedReason(str, Enum):
    QC_FAILED = 'QC failed'
    COLLECTION_NOT_READY = 'collection not ready'
    METADATA_INVALID = 'metadata invalid'
    RECORD_RETRACTED = 'record retracted'
    MIGRATED_NOT_PUBLISHED = 'migrated as not published'
    NO_DOI = 'no DOI'
    NOT_MIMS_COLLECTION = 'not a MIMS collection'


class Catalog:
    indexed = False
    """Whether to save indexing data to catalog records.
    If true, methods create_text_index_data, create_keyword_index_data,
    create_spatial_index_data and create_temporal_index_data must be implemented.
    """

    external = False
    """Whether to synchronize catalog records with an external system.
    If true, method sync_external_record must be implemented.
    """

    max_attempts = 3
    """Maximum number of consecutive attempts at externally synchronizing a record."""

    def __init__(self, catalog_id: str, cache: Cache) -> None:
        self.catalog_id = catalog_id
        self.cache = cache

    @final
    def publish(self) -> None:
        records = self._select_records()
        logger.info(f'{self.catalog_id} catalog: {(total := len(records))} records selected for evaluation')

        if total:
            self._create_snapshot(records)

            logger.debug(f'{self.catalog_id} catalog: Synchronizing catalog records...')
            published = 0
            for record_id, timestamp in records:
                published += self._sync_catalog_record(record_id, timestamp)

            logger.info(f'{self.catalog_id} catalog: {published} records published; {total - published} records hidden')

        if self.external:
            self._sync_external()

    def _select_records(self) -> list[tuple[str, datetime]]:
        """Select records to be evaluated for publication to, or
        retraction from, a catalog.

        A record is selected if:

        * there is no corresponding catalog_record entry; or
        * the record has any embargo tags; or
        * catalog_record.timestamp is less than any of:

          * collection.timestamp
          * provider.timestamp
          * record.timestamp

        :return: a list of (record_id, timestamp) tuples, where
            timestamp is that of the latest contributing change
        """
        records_subq = (
            select(
                Record.id.label('record_id'),
                func.greatest(
                    Collection.timestamp,
                    Provider.timestamp,
                    Record.timestamp,
                ).label('max_timestamp')
            ).
            join(Collection).
            join(Provider).
            subquery()
        )

        catalog_records_subq = (
            select(
                CatalogRecord.record_id,
                CatalogRecord.timestamp
            ).
            where(CatalogRecord.catalog_id == self.catalog_id).
            subquery()
        )

        stmt = (
            select(
                records_subq.c.record_id,
                records_subq.c.max_timestamp
            ).
            outerjoin_from(records_subq, catalog_records_subq).
            where(or_(
                catalog_records_subq.c.record_id == None,
                catalog_records_subq.c.timestamp < records_subq.c.max_timestamp,
                catalog_records_subq.c.record_id.in_(
                    select(RecordTag.record_id).
                    where(RecordTag.tag_id == ODPRecordTag.EMBARGO)
                )
            ))
        )

        return Session.execute(stmt).all()

    def _create_snapshot(self, records: list[tuple[str, datetime]]) -> None:
        """Create a snapshot of API record output models for the selected
        records.

        To ensure consistency of lookup info across all catalog records,
        the DB engine should be created with a transaction isolation level
        of 'REPEATABLE READ'.
        """
        logger.debug(f'{self.catalog_id} catalog: Creating snapshot...')
        for record_id, _ in records:
            record = Session.get(Record, record_id)
            record_model = output_record_model(record)
            self.cache.jset(record_id, value=record_model.dict())

    def _sync_catalog_record(self, record_id: str, timestamp: datetime) -> bool:
        """Synchronize a catalog_record entry with the current state of the
        corresponding record.

        The catalog_record entry is stamped with the `timestamp` of the latest
        contributing change (from record / collection / provider).
        """
        catalog_record = (Session.get(CatalogRecord, (self.catalog_id, record_id)) or
                          CatalogRecord(catalog_id=self.catalog_id, record_id=record_id))

        cached_record_dict = self.cache.jget(record_id, expire=True)
        record_model = RecordModel(**cached_record_dict)

        can_publish, reasons = self.evaluate_record(record_model)
        if can_publish:
            self._process_embargoes(record_model)
            self._save_published_doi(record_model)
            catalog_record.published = True
            catalog_record.published_record = self.create_published_record(record_model).dict()
        else:
            catalog_record.published = False
            catalog_record.published_record = None

        if self.indexed:
            self._index_catalog_record(catalog_record)

        if self.external:
            catalog_record.synced = False
            catalog_record.error = None
            catalog_record.error_count = 0

        catalog_record.reason = ' | '.join(reasons)
        catalog_record.timestamp = timestamp
        catalog_record.save()
        Session.commit()

        return catalog_record.published

    def evaluate_record(self, record_model: RecordModel) -> tuple[bool, list[PublishedReason | NotPublishedReason]]:
        """Evaluate whether a record can be published.

        Universal rules are defined here; derived Catalog classes
        may extend these with catalog-specific rules.

        :return: tuple(can_publish: bool, reasons: list)
        """
        # tag for a record migrated without any subsequent changes
        migrated_tag = next(
            (tag for tag in record_model.tags if tag.tag_id == ODPRecordTag.MIGRATED and
             datetime.fromisoformat(tag.timestamp) >= datetime.fromisoformat(record_model.timestamp)),
            None
        )
        collection_ready = any(
            (tag for tag in record_model.tags if tag.tag_id == ODPCollectionTag.READY)
        )
        qc_passed = any(
            (tag for tag in record_model.tags if tag.tag_id == ODPRecordTag.QC and tag.data['pass_'])
        ) and not any(
            (tag for tag in record_model.tags if tag.tag_id == ODPRecordTag.QC and not tag.data['pass_'])
        )
        retracted = any(
            (tag for tag in record_model.tags if tag.tag_id == ODPRecordTag.RETRACTED)
        )
        metadata_valid = record_model.validity['valid']

        published_reasons = []
        not_published_reasons = []

        # collection readiness applies to both migrated and non-migrated records
        if collection_ready:
            published_reasons += [PublishedReason.COLLECTION_READY]
        else:
            not_published_reasons += [NotPublishedReason.COLLECTION_NOT_READY]

        if migrated_tag:
            if migrated_tag.data['published']:
                published_reasons += [PublishedReason.MIGRATED_PUBLISHED]
            else:
                not_published_reasons += [NotPublishedReason.MIGRATED_NOT_PUBLISHED]

        else:
            if qc_passed:
                published_reasons += [PublishedReason.QC_PASSED]
            else:
                not_published_reasons += [NotPublishedReason.QC_FAILED]

            if retracted:
                not_published_reasons += [NotPublishedReason.RECORD_RETRACTED]

            if not metadata_valid:
                not_published_reasons += [NotPublishedReason.METADATA_INVALID]

        if not_published_reasons:
            return False, not_published_reasons

        return True, published_reasons

    def create_published_record(self, record_model: RecordModel) -> PublishedRecordModel:
        """Create the published form of a record."""
        raise NotImplementedError

    @staticmethod
    def _process_embargoes(record_model: RecordModel) -> None:
        """Check if a record is currently subject to an embargo and, if so, update
        the given `record_model`, stripping out download links / embedded datasets
        from the metadata."""
        current_date = date.today()
        embargoed = False

        for tag in record_model.tags:
            if tag.tag_id == ODPRecordTag.EMBARGO:
                start_date = date.fromisoformat(tag.data['start'])
                end_date = date.fromisoformat(tag.data['end'] or '3000-01-01')
                if start_date <= current_date <= end_date:
                    embargoed = True
                    break

        if not embargoed:
            return

        if record_model.schema_id == ODPMetadataSchema.SAEON_DATACITE4:
            try:
                if 'resourceDownload' in record_model.metadata['immutableResource']:
                    record_model.metadata['immutableResource']['resourceDownload']['downloadURL'] = None
            except KeyError:
                pass
            try:
                if 'resourceData' in record_model.metadata['immutableResource']:
                    record_model.metadata['immutableResource']['resourceData'] = None
            except KeyError:
                pass

        elif record_model.schema_id == ODPMetadataSchema.SAEON_ISO19115:
            for item in record_model.metadata.get('onlineResources', []):
                try:
                    if item['description'] == 'download':
                        item['linkage'] = None
                except KeyError:
                    pass

    @staticmethod
    def _save_published_doi(record_model: RecordModel) -> None:
        """Permanently save a DOI when it is first published."""
        if record_model.doi and not Session.get(PublishedDOI, record_model.doi):
            published_doi = PublishedDOI(doi=record_model.doi)
            published_doi.save()

    def _sync_external(self) -> None:
        """Synchronize with an external catalog."""
        unsynced_catalog_records = Session.execute(
            select(CatalogRecord).
            where(CatalogRecord.catalog_id == self.catalog_id).
            where(CatalogRecord.synced == False).
            where(CatalogRecord.error_count < self.max_attempts)
        ).scalars().all()

        logger.info(f'{self.catalog_id} catalog: {(total := len(unsynced_catalog_records))} records selected for external sync')
        synced = 0

        for catalog_record in unsynced_catalog_records:
            try:
                self.sync_external_record(catalog_record.record_id)
                catalog_record.synced = True
                catalog_record.error = None
                catalog_record.error_count = 0
                synced += 1
            except Exception as e:
                catalog_record.error = repr(e)
                catalog_record.error_count += 1

            catalog_record.save()
            Session.commit()

        if total:
            logger.info(f'{self.catalog_id} catalog: {synced} records synced; {total - synced} errors')

    def sync_external_record(self, record_id: str) -> None:
        """Create / update / delete a record on an external catalog."""

    def _index_catalog_record(self, catalog_record: CatalogRecord) -> None:
        """Compute and store search data for a catalog record."""
        if catalog_record.published:
            published_record = output_published_record_model(catalog_record)

            catalog_record.full_text = select(
                func.to_tsvector('english', self.create_text_index_data(published_record))
            ).scalar_subquery()

            catalog_record.keywords = self.create_keyword_index_data(published_record)

            if north_east_south_west := self.create_spatial_index_data(published_record):
                (catalog_record.spatial_north,
                 catalog_record.spatial_east,
                 catalog_record.spatial_south,
                 catalog_record.spatial_west) = north_east_south_west

            if start_end := self.create_temporal_index_data(published_record):
                (catalog_record.temporal_start,
                 catalog_record.temporal_end) = start_end

        else:
            catalog_record.full_text = None
            catalog_record.keywords = None
            catalog_record.spatial_north = None
            catalog_record.spatial_east = None
            catalog_record.spatial_south = None
            catalog_record.spatial_west = None
            catalog_record.temporal_start = None
            catalog_record.temporal_end = None

    def create_text_index_data(self, published_record: PublishedRecordModel) -> str:
        """Create a string from metadata field values to be indexed for full text search."""

    def create_keyword_index_data(self, published_record: PublishedRecordModel) -> list[str]:
        """Create an array of metadata keywords to be indexed for keyword search."""

    def create_spatial_index_data(self, published_record: PublishedRecordModel) -> tuple[float, float, float, float]:
        """Create a N-E-S-W tuple of the spatial extent to be indexed for spatial search."""

    def create_temporal_index_data(self, published_record: PublishedRecordModel) -> tuple[Optional[datetime], Optional[datetime]]:
        """Create a start-end tuple of the temporal extent to be indexed for temporal search."""


def publish_all():
    from odp.catalog.datacite import DataCiteCatalog
    from odp.catalog.mims import MIMSCatalog
    from odp.catalog.saeon import SAEONCatalog

    catalog_classes = {
        ODPCatalog.SAEON: SAEONCatalog,
        ODPCatalog.MIMS: MIMSCatalog,
        ODPCatalog.DATACITE: DataCiteCatalog,
    }

    logger.info('PUBLISHING STARTED')
    try:
        cache = Cache(__name__)

        for catalog_id, catalog_cls in catalog_classes.items():
            catalog_cls(catalog_id, cache).publish()

        logger.info('PUBLISHING FINISHED')

    except Exception as e:
        logger.critical(f'PUBLISHING ABORTED: {str(e)}')
