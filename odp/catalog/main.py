import logging

from sqlalchemy import select

from odp.catalog.datacite import DataCitePublisher
from odp.catalog.saeon import SAEONPublisher
from odp.const import ODPCatalog
from odp.db import Session
from odp.db.models import Catalog
from odp.logfile import init_logging

init_logging()

logger = logging.getLogger(__name__)

publishers = {
    ODPCatalog.SAEON: SAEONPublisher,
    ODPCatalog.DATACITE: DataCitePublisher,
}


def publish():
    logger.info('PUBLISHING STARTED')
    try:
        for catalog_id in Session.execute(select(Catalog.id)).scalars():
            publisher = publishers[catalog_id]
            publisher(catalog_id).run()

        logger.info('PUBLISHING FINISHED')

    except Exception as e:
        logger.critical(f'PUBLISHING ABORTED: {str(e)}')
