from odp.api.models import PublishedSAEONRecordModel, RecordModel
from odp.catalog.saeon import SAEONCatalog
from odp.const import ODPCollectionTag, ODPMetadataSchema


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

    def create_facet_index_data(
            self, published_record: PublishedSAEONRecordModel
    ) -> dict[str, list[str]]:
        """Create a mapping of facet names to values to be indexed for faceted search."""
        facets = super().create_facet_index_data(published_record)
        facets |= {
            'Project': [],  # replaces SAEON 'Project' facet
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
