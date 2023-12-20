from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from data_pull_tools.file_utils import clear_dir, make_file_not_found_error

from .resolve_strategy import DEFAULT_RESOLVE_STRATEGY, ResolveStrategy

if TYPE_CHECKING:
    from . import Cacher, Pathish

module_logger = logging.getLogger(__name__)


class CacheManager:
    _root_dir: Path
    _cache_dir: Path
    _raw_cache_dir: Pathish | None
    _cache_resolver: ResolveStrategy

    def __init__(
        self,
        root_dir: Pathish | None = None,
        cache_dir: Pathish | None = None,
        *,
        cache_resolver: ResolveStrategy | None = None,
    ) -> None:
        # Set internal values
        self._root_dir = self._validate_root_dir(root_dir)
        self._cache_resolver = cache_resolver or DEFAULT_RESOLVE_STRATEGY
        self._raw_cache_dir = cache_dir
        self._cache_dir = self.cache_resolver(self.root_dir, cache_dir)

    #
    # Properties and property validating methods
    #

    @staticmethod
    def _validate_root_dir(root_dir: Pathish | None) -> Path:
        """Validates and returns the root directory.

        Parameters
        ----------
        root_dir : Pathish | None
            The root directory to validate.

        Returns
        -------
        Path
            The validated root directory.

        Raises
        ------
        FileNotFoundError
            If the root directory does not exist.
        TypeError
            If the root directory is not a directory.
        """
        root_dir = Path() if root_dir is None else Path(root_dir)

        if not root_dir.exists():
            err = make_file_not_found_error(root_dir)
            raise err

        if not root_dir.is_dir():
            msg = "'root_dir' must be a directory, received: %s"
            raise TypeError(msg, root_dir)

        return root_dir

    @property
    def root_dir(self) -> Path:
        """Get or set the root directory.

        When setting, validates the input and updates the cache directory as necessary.
        """
        return self._root_dir

    @root_dir.setter
    def root_dir(self, root_dir: Pathish | None) -> None:
        root_dir = self._validate_root_dir(root_dir)
        if root_dir == self._root_dir:
            return  # No change
        self._root_dir = root_dir
        self.cache_dir = self._raw_cache_dir  # Revalidate cache_dir

    @property
    def cache_dir(self) -> Path:
        """Get or set the cache directory.

        When setting, validates the input.
        """
        return self._cache_dir

    @cache_dir.setter
    def cache_dir(self, cache_dir: Pathish | None) -> None:
        if cache_dir == self._raw_cache_dir:
            return  # No change
        self._raw_cache_dir = cache_dir
        self._cache_dir = self.cache_resolver(self.root_dir, cache_dir)

    @property
    def cache_resolver(self) -> ResolveStrategy:
        """Get or set the cache location.

        When setting, updates the cache directory as necessary..
        """
        return self._cache_resolver

    @cache_resolver.setter
    def cache_resolver(self, cache_resolve: ResolveStrategy) -> None:
        if cache_resolve == self._cache_resolver:
            return  # No change
        self._cache_resolver = cache_resolve
        self.cache_dir = self._raw_cache_dir  # Revalidate cache_dir

    #
    # Methods
    #

    def clear_cache(self):
        clear_dir(self.cache_dir)

    def _input_path(
        self,
        input_file: Pathish,
    ) -> Path:
        if isinstance(input_file, str):
            return self.root_dir / input_file
        return Path(input_file)

    def output_path(
        self,
        cache_file: Pathish,
        cacher: Cacher,
    ) -> Path:
        if isinstance(cache_file, str):
            return self.cache_dir / f"{cache_file}{cacher.suffix}"
        cache_file = Path(cache_file)
        return cache_file.parent / f"{cache_file.stem}{cacher.suffix}"
