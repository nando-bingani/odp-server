from odp.api.models import PublishedMetadataModel, PublishedRecordModel, PublishedSAEONRecordModel, RecordModel
from odp.catalog.saeon import SAEONCatalog
from odp.const import ODPCatalog, ODPCollectionTag, ODPMetadataSchema
from odp.db import Session
from odp.db.models import Catalog, CatalogRecord, Schema, SchemaType


class MIMSCatalog(SAEONCatalog):

    def evaluate_record(
            self,
            record_model: RecordModel,
            can_publish_reasons: list[str],
            cannot_publish_reasons: list[str],
    ) -> None:
        """Evaluate whether a record can be published.

        A record can be published only if it belongs to a collection
        with a 'MIMS' infrastructure tag.
        """
        super().evaluate_record(record_model, can_publish_reasons, cannot_publish_reasons)

        tagged_mims = any((
            tag for tag in record_model.tags
            if tag.tag_id == ODPCollectionTag.INFRASTRUCTURE and tag.data['infrastructure'] == 'MIMS'
        ))

        if tagged_mims:
            can_publish_reasons += ['MIMS collection']
        else:
            cannot_publish_reasons += ['not a MIMS collection']

    def create_published_record(self, record_model: RecordModel) -> PublishedRecordModel:
        published_record: PublishedSAEONRecordModel = super().create_published_record(record_model)

        for child_doi, child_id in record_model.child_dois.items():
            if child_record_model := self.snapshot.get(child_id):
                can_publish_reasons = []
                cannot_publish_reasons = []
                self.evaluate_record(child_record_model, can_publish_reasons, cannot_publish_reasons)
                is_child_published = not cannot_publish_reasons
            else:
                catalog_record = Session.get(CatalogRecord, (self.catalog_id, child_id))
                is_child_published = catalog_record.published

            if is_child_published:
                for metadata_record in published_record.metadata_records:
                    metadata_record.metadata.setdefault('relatedIdentifiers', [])
                    metadata_record.metadata['relatedIdentifiers'] += [{
                        'relatedIdentifier': child_doi,
                        'relatedIdentifierType': 'DOI',
                        'relationType': 'HasPart',
                    }]

        mims_catalog = Session.get(Catalog, ODPCatalog.MIMS)
        schemaorg_schema = Session.get(Schema, (ODPMetadataSchema.SCHEMAORG_DATASET, SchemaType.metadata))
        datacite_metadata = self._get_metadata_dict(published_record, ODPMetadataSchema.SAEON_DATACITE4)

        title = next(
            (t.get('title') for t in datacite_metadata.get('titles', ())),
            ''
        )
        abstract = next(
            (d.get('description') for d in datacite_metadata.get('descriptions', ())
             if d.get('descriptionType') == 'Abstract'),
            ''
        )
        identifier = f'doi:{published_record.doi}' if published_record.doi else None
        license = next(
            (r.get('rightsURI') for r in datacite_metadata.get('rightsList', ())),
            ''
        )
        url = (f'{mims_catalog.url}/'
               f'{published_record.doi if published_record.doi else published_record.id}')

        published_record.metadata_records += [
            PublishedMetadataModel(
                schema_id=schemaorg_schema.id,
                schema_uri=schemaorg_schema.uri,
                metadata={
                    '@context': 'https://schema.org/',
                    '@type': 'Dataset',
                    'name': title,
                    'description': abstract,
                    'identifier': identifier,
                    'keywords': self.create_keyword_index_data(published_record),
                    'license': license,
                    'url': url,
                }
            )
        ]

        return published_record

    def create_facet_index_data(
            self, published_record: PublishedSAEONRecordModel
    ) -> dict[str, list[str]]:
        """Create a mapping of facet names to values to be indexed for faceted search."""
        facets = super().create_facet_index_data(published_record)
        facets.pop('Collection')
        facets.pop('Product')
        facets |= {
            'Project': [],
            'Location': [],
            'Instrument': [],
        }
        iso19115_facets = {
            'theme': 'Project',
            'place': 'Location',
            'stratum': 'Instrument',
        }
        if iso19115_metadata := self._get_metadata_dict(published_record, ODPMetadataSchema.SAEON_ISO19115):
            for keyword_obj in iso19115_metadata.get('descriptiveKeywords', ()):
                if (keyword_type := keyword_obj.get('keywordType')) in ('theme', 'place', 'stratum'):
                    facets[iso19115_facets[keyword_type]] += [keyword_obj.get('keyword', '')]

        return facets
