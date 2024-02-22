import logging

logger = logging.getLogger(__name__)


class Archive:
    """Archival service."""

    def __init__(self, archive_id: str) -> None:
        self.archive_id = archive_id
