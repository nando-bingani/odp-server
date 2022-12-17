from datetime import datetime
from typing import Optional

from jschon import JSON, URI

from odp.api.models import PublishedMetadataModel, PublishedRecordModel, PublishedSAEONRecordModel, PublishedTagInstanceModel, RecordModel
from odp.catalog import Catalog
from odp.const import ODPMetadataSchema
from odp.db import Session
from odp.db.models import Schema, SchemaType
from odp.lib.schema import schema_catalog


class SAEONCatalog(Catalog):
    indexed = True

    def create_published_record(self, record_model: RecordModel) -> PublishedRecordModel:
        """Create the published form of a record."""
        return PublishedSAEONRecordModel(
            id=record_model.id,
            doi=record_model.doi,
            sid=record_model.sid,
            collection_key=record_model.collection_key,
            collection_name=record_model.collection_name,
            provider_key=record_model.provider_key,
            provider_name=record_model.provider_name,
            metadata=self._create_published_metadata(record_model),
            tags=self._create_published_tags(record_model),
            timestamp=record_model.timestamp,
        )

    @staticmethod
    def _create_published_metadata(record_model: RecordModel) -> list[PublishedMetadataModel]:
        """Create the published metadata outputs for a record."""
        published_metadata = [
            PublishedMetadataModel(
                schema_id=record_model.schema_id,
                metadata=record_model.metadata,
            )
        ]

        if record_model.schema_id == ODPMetadataSchema.SAEON_ISO19115:
            schema = Session.get(Schema, (record_model.schema_id, SchemaType.metadata))
            iso19115_schema = schema_catalog.get_schema(URI(schema.uri))
            result = iso19115_schema.evaluate(JSON(record_model.metadata))
            datacite_metadata = result.output('translation', scheme='saeon/datacite4', ignore_validity=True)
            published_metadata += [
                PublishedMetadataModel(
                    schema_id=ODPMetadataSchema.SAEON_DATACITE4,
                    metadata=datacite_metadata,
                )
            ]

        return published_metadata

    @staticmethod
    def _create_published_tags(record_model: RecordModel) -> list[PublishedTagInstanceModel]:
        """Create the published tags for a record."""
        return [
            PublishedTagInstanceModel(
                tag_id=tag_instance.tag_id,
                data=tag_instance.data,
                user_name=tag_instance.user_name,
                timestamp=tag_instance.timestamp,
            ) for tag_instance in record_model.tags if tag_instance.public
        ]

    def create_text_index_data(self, published_record: PublishedSAEONRecordModel) -> str:
        """Create a string from metadata field values to be indexed for full text search."""
        datacite_metadata = self._get_datacite_metadata(published_record)
        values = []

        for title in datacite_metadata.get('titles', ()):
            if title_text := title.get('title'):
                values += [title_text]

        if publisher := datacite_metadata.get('publisher'):
            values += [publisher]

        for creator in datacite_metadata.get('creators', ()):
            if creator_name := creator.get('name'):
                values += [creator_name]
            for affiliation in creator.get('affiliation', ()):
                if affiliation_text := affiliation.get('affiliation'):
                    values += [affiliation_text]

        for contributor in datacite_metadata.get('contributors', ()):
            if contributor_name := contributor.get('name'):
                values += [contributor_name]
            for affiliation in contributor.get('affiliation', ()):
                if affiliation_text := affiliation.get('affiliation'):
                    values += [affiliation_text]

        for subject in datacite_metadata.get('subjects', ()):
            if subject_text := subject.get('subject'):
                values += [subject_text]

        for description in datacite_metadata.get('descriptions', ()):
            if description_text := description.get('description'):
                values += [description_text]

        return ' '.join(values)

    def create_keyword_index_data(self, published_record: PublishedSAEONRecordModel) -> list[str]:
        """Create an array of metadata keywords to be indexed for keyword search."""
        pass

    def create_spatial_index_data(self, published_record: PublishedSAEONRecordModel) -> tuple[float, float, float, float]:
        """Create a N-E-S-W tuple of the spatial extent to be indexed for spatial search."""
        pass

    def create_temporal_index_data(self, published_record: PublishedSAEONRecordModel) -> tuple[Optional[datetime], Optional[datetime]]:
        """Create a start-end tuple of the temporal extent to be indexed for temporal search."""

        def _get_dt(n):
            try:
                return datetime.fromisoformat(date_values[n])
            except (IndexError, ValueError):
                pass

        datacite_metadata = self._get_datacite_metadata(published_record)
        start = None
        end = None

        for date_obj in datacite_metadata.get('dates', ()):
            if date_obj.get('dateType') == 'Valid':
                if date_text := date_obj.get('date'):
                    date_values = date_text.split('/')
                    if start_dt := _get_dt(0):
                        if not start or start_dt < start:
                            start = start_dt
                    if end_dt := _get_dt(1):
                        if not end or end_dt > end:
                            end = end_dt

        return start, end

    @staticmethod
    def _get_datacite_metadata(published_record: PublishedSAEONRecordModel) -> dict:
        return next((
            published_metadata.metadata
            for published_metadata in published_record.metadata
            if published_metadata.schema_id == ODPMetadataSchema.SAEON_DATACITE4
        ))
