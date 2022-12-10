import logging

from odp.catalog.publisher.datacite import DataCitePublisher
from odp.catalog.publisher.mims import MIMSPublisher
from odp.catalog.publisher.saeon import SAEONPublisher
from odp.const import ODPCatalog

logger = logging.getLogger(__name__)

publishers = {
    ODPCatalog.SAEON: SAEONPublisher,
    ODPCatalog.MIMS: MIMSPublisher,
    ODPCatalog.DATACITE: DataCitePublisher,
}


def publish():
    logger.info('PUBLISHING STARTED')
    try:
        for catalog_id, publisher_cls in publishers.items():
            publisher_cls(catalog_id).run()

        logger.info('PUBLISHING FINISHED')

    except Exception as e:
        logger.critical(f'PUBLISHING ABORTED: {str(e)}')
