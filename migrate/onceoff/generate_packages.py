import os

from odp.config import config
from odp.const import ODPScope
from odp.lib.client import ODPClient


def create_packages_from_records():
    cli = ODPClient(
        api_url=config.ODP.API_URL,
        hydra_url=config.HYDRA.PUBLIC.URL,
        client_id='ODP.Migrate',
        client_secret=os.environ['ODP_MIGRATE_CLIENT_SECRET'],
        scope=[
            ODPScope.RECORD_READ,
            ODPScope.ARCHIVE_READ,
            ODPScope.PACKAGE_WRITE,
            ODPScope.RESOURCE_WRITE,
        ],
    )
    # TODO...
