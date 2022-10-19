from odp.config import config
from odp.const import DOI_PREFIX
from odp.lib.datacite import DataciteClient


async def get_datacite_client() -> DataciteClient:
    return DataciteClient(
        api_url=config.DATACITE.API_URL,
        username=config.DATACITE.USERNAME,
        password=config.DATACITE.PASSWORD,
        doi_prefix=DOI_PREFIX,
    )
