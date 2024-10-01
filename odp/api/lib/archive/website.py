from os import PathLike
from urllib.parse import urljoin

from fastapi.responses import RedirectResponse

from odp.api.lib.archive import ArchiveAdapter


class WebsiteArchiveAdapter(ArchiveAdapter):
    """Adapter for a read-only archive with its own web interface
    for accessing data."""

    async def get(self, path: str | PathLike) -> RedirectResponse:
        """Return a redirect to the relevant web page."""
        return RedirectResponse(urljoin(self.download_url, path))
