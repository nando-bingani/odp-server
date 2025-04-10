from os import PathLike
from urllib.parse import urljoin

from odp.lib.archive import ArchiveAdapter, ArchiveRedirectResponse


class WebsiteArchiveAdapter(ArchiveAdapter):
    """Adapter for a read-only archive with its own web interface
    for accessing data."""

    async def get(self, path: str | PathLike) -> ArchiveRedirectResponse:
        """Return a redirect to the relevant web page."""
        return ArchiveRedirectResponse(urljoin(self.download_url, path))
