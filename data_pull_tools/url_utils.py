from pathlib import PurePosixPath
from urllib.parse import SplitResult, urlsplit, urlunsplit


class URL:
    @staticmethod
    def _absolute_url(splits: SplitResult) -> bool:
        return all([splits.scheme, splits.netloc])

    def __init__(self, url: str) -> None:
        self._parts = urlsplit(url)
        self._path = PurePosixPath(self._parts.path)

        if not self._absolute_url(self._parts):
            msg = f"{url!r} could not be interpreted as an absolute URL."
            raise ValueError(msg)
        self._url = urlunsplit(self._parts)

    def __str__(self) -> str:
        return self._url

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self!s}')"

    def __truediv__(self, path: str) -> "URL":
        splits = urlsplit(path)
        ignored_parts = splits[:2] + splits[3:]
        if any(part != "" for part in ignored_parts):
            msg = f"Can only join a purely relative path but got {splits}"
            raise ValueError(msg)
        new_path = self._path / splits.path
        new_parts = self._parts._replace(path=str(new_path))
        return URL(urlunsplit(new_parts))
