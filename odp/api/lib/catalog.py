from pydantic import constr

from odp.config import config
from odp.const import DOI_REGEX


async def get_catalog_url(doi: constr(regex=DOI_REGEX)) -> str:
    return f'{config.ODP.CATALOG_URL}/{doi}'
