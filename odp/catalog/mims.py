from odp.api.models import RecordModel
from odp.catalog import NotPublishedReason, PublishedReason
from odp.catalog.saeon import SAEONCatalog
from odp.const import ODPCollectionTag
from odp.db import Session
from odp.db.models import Collection


class MIMSCatalog(SAEONCatalog):

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

    def evaluate_collection(self, collection_id: str) -> bool:
        """Evaluate whether a collection can be published.

        A collection can be published only if it is tagged with a
        'MIMS' infrastructure tag.
        """
        can_publish = super().evaluate_collection(collection_id)
        if can_publish:
            collection = Session.get(Collection, collection_id)
            can_publish = any((
                tag for tag in collection.tags
                if tag.tag_id == ODPCollectionTag.INFRASTRUCTURE and tag.data['infrastructure'] == 'MIMS'
            ))

        return can_publish
