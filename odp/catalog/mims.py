from odp.api.models import RecordModel
from odp.catalog import NotPublishedReason, PublishedReason
from odp.catalog.saeon import SAEONPublisher


class MIMSPublisher(SAEONPublisher):

    def evaluate_record(self, record_model: RecordModel) -> tuple[bool, list[PublishedReason | NotPublishedReason]]:
        """Evaluate whether a record can be published.

        Only publish records belonging to collections with a 'MIMS' infrastructure tag.

        :return: tuple(can_publish: bool, reasons: list)
        """
        can_publish, reasons = super().evaluate_record(record_model)

        # TODO...

        return can_publish, reasons
