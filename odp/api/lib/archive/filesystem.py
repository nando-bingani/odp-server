import shutil
from os import PathLike
from pathlib import Path
from urllib.parse import urlparse

from fastapi import HTTPException, UploadFile
from fastapi.responses import FileResponse
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from odp.api.lib.archive import ArchiveAdapter


class FileSystemArchiveAdapter(ArchiveAdapter):
    """Adapter for a Unix file system archive."""

    def __init__(self, url: str | PathLike) -> None:
        super().__init__(url)
        self.dir = Path(urlparse(url).path)

    def get(self, path: str | PathLike) -> FileResponse:
        """Send the contents of the file at `path` to the client."""
        return FileResponse(self.dir / path)

    def put(self, path: str | PathLike, file: UploadFile) -> None:
        """Store the contents of the incoming `file` at `path`."""
        try:
            (self.dir / path).parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        except OSError as e:
            raise HTTPException(
                HTTP_422_UNPROCESSABLE_ENTITY, f'Error creating directory at {Path(path).parent}: {e}'
            )

        try:
            file.seek(0)
            with open(self.dir / path, 'wb') as f:
                shutil.copyfileobj(file.file, f)
        except OSError as e:
            raise HTTPException(
                HTTP_422_UNPROCESSABLE_ENTITY, f'Error creating file at {path}: {e}'
            )
