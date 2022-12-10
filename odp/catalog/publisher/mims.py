from odp.api.models import RecordModel
from odp.catalog.publisher import NotPublishedReason, PublishedReason
from odp.catalog.publisher.saeon import SAEONPublisher
from odp.const import ODPCollectionTag


class MIMSPublisher(SAEONPublisher):

    def evaluate_record(self, record_model: RecordModel) -> tuple[bool, list[PublishedReason | NotPublishedReason]]:
        """Evaluate whether a record can be published.

        A record can be published only if it belongs to a collection
        with a 'MIMS' infrastructure tag.

        :return: tuple(can_publish: bool, reasons: list)
        """
        can_publish, reasons = super().evaluate_record(record_model)

        tagged_mims = any((
            tag for tag in record_model.tags
            if tag.tag_id == ODPCollectionTag.INFRASTRUCTURE and tag.data['infrastructure'] == 'MIMS'
        ))

        if can_publish:
            if tagged_mims:
                reasons += [PublishedReason.MIMS_COLLECTION]
            else:
                can_publish = False
                reasons = [NotPublishedReason.NOT_MIMS_COLLECTION]

        elif not tagged_mims:
            reasons += [NotPublishedReason.NOT_MIMS_COLLECTION]

        return can_publish, reasons
