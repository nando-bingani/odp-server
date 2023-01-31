from odp.api.models import RecordModel
from odp.catalog.saeon import SAEONCatalog
from odp.const import ODPCollectionTag


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
