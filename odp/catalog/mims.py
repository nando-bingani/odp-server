from typing import Any, Iterator

from sqlalchemy import select

from odp.api.models import PublishedMetadataModel, PublishedRecordModel, PublishedSAEONRecordModel, RecordModel
from odp.catalog.saeon import SAEONCatalog
from odp.const import ODPCollectionTag, ODPMetadataSchema
from odp.const.db import SchemaType
from odp.db import Session
from odp.db.models import Catalog, CatalogRecord, Schema


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
        """Create the published form of a record."""
        published_record: PublishedSAEONRecordModel = super().create_published_record(record_model)

        # add related identifiers for published child records to
        # the DataCite and ISO19115 metadata records
        for child_doi, child_id in record_model.child_dois.items():
            if child_snapshot := self.snapshot.get(child_id):
                child_record_model, _ = child_snapshot
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

        # add a JSON-LD metadata record
        schemaorg_schema = Session.get(Schema, (ODPMetadataSchema.SCHEMAORG_DATASET, SchemaType.metadata))
        published_record.metadata_records += [
            PublishedMetadataModel(
                schema_id=schemaorg_schema.id,
                schema_uri=schemaorg_schema.uri,
                metadata=self._create_jsonld_metadata(published_record)
            )
        ]

        # add an RIS citation record
        ris_schema = Session.get(Schema, (ODPMetadataSchema.RIS_CITATION, SchemaType.metadata))
        published_record.metadata_records += [
            PublishedMetadataModel(
                schema_id=ris_schema.id,
                schema_uri=ris_schema.uri,
                metadata=self._create_ris_metadata(published_record)
            )
        ]

        return published_record

    def _create_jsonld_metadata(
            self, published_record: PublishedSAEONRecordModel
    ) -> dict[str, Any]:
        """Create a JSON-LD metadata dictionary, using the schema.org vocabulary."""
        mims_catalog = Session.get(Catalog, self.catalog_id)
        datacite_metadata = self._get_metadata_dict(published_record, ODPMetadataSchema.SAEON_DATACITE4)

        title = next(
            (t.get('title') for t in datacite_metadata.get('titles', ())),
            None
        )
        abstract = next(
            (d.get('description') for d in datacite_metadata.get('descriptions', ())
             if d.get('descriptionType') == 'Abstract'),
            None
        )
        identifier = (
            f'doi:{published_record.doi}' if published_record.doi else None
        )
        license = next(
            (r.get('rightsURI') for r in datacite_metadata.get('rightsList', ())),
            None
        )
        url = (
            f'{mims_catalog.url}/'
            f'{published_record.doi if published_record.doi else published_record.id}'
        )
        temporal_cov = next(
            (d.get('date') for d in datacite_metadata.get('dates', ())
             if d.get('dateType') == 'Valid'),
            None
        )

        jsonld_metadata = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": url,
            "name": title,
            "description": abstract,
            "identifier": identifier,
            "keywords": self.create_keyword_index_data(published_record),
            "license": license,
            "url": url,
            "temporalCoverage": temporal_cov,
        }

        # Create a GeoShape box from DataCite geoLocationBox, because currently
        # we only use bounding boxes in our metadata. If in future we ever use
        # geoLocationPolygons, then this code will need to be adapted.
        if geolocation_box := next(
                (g.get('geoLocationBox') for g in datacite_metadata.get('geoLocations', ())),
                None
        ):
            box = (
                f'{geolocation_box["southBoundLatitude"]} '
                f'{geolocation_box["westBoundLongitude"]} '
                f'{geolocation_box["northBoundLatitude"]} '
                f'{geolocation_box["eastBoundLongitude"]}'
            )
            jsonld_metadata |= {
                "spatialCoverage": {
                    "@type": "Place",
                    "geo": {
                        "@type": "GeoShape",
                        "box": box,
                    },
                },
            }

        return jsonld_metadata

    def _create_ris_metadata(
            self, published_record: PublishedSAEONRecordModel
    ) -> dict[str, Any]:
        """Create a metadata dictionary consisting of a single "ris" text
        property with an RIS-format citation."""
        return dict(
            ris=''
        )

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

    def create_global_data(self) -> Any:
        """Create a JSON-compatible object to be published as global data for the catalog."""
        return {
            'sitemap.xml': self._create_sitemap_xml()
        }

    def _create_sitemap_xml(self) -> str:
        return f'''<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            {''.join(self._iter_sitemap_urls())}
        </urlset>'''

    def _iter_sitemap_urls(self) -> Iterator[str]:
        for row in Session.execute(
                select(
                    Catalog.url,
                    CatalogRecord.published_record['doi'].label('doi'),
                    CatalogRecord.record_id,
                    CatalogRecord.timestamp,
                )
                .join(CatalogRecord)
                .where(CatalogRecord.catalog_id == self.catalog_id)
                .where(CatalogRecord.published)
                .where(CatalogRecord.searchable)
        ):
            yield f'''<url>
                <loc>{row.url}/{row.doi if row.doi else row.record_id}</loc>
                <lastmod>{row.timestamp}</lastmod>
            </url>'''
