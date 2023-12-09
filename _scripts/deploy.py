from pathlib import Path
from typing import Any

from data_pull_tools.toml_utils import CachedTOMLReader
from git.repo import Repo

_reader = CachedTOMLReader()
_project_root = Path(__file__).parent.parent


def load_pyproject() -> dict[str, Any]:
    return _reader.load(_project_root / "pyproject.toml")


def v_tag_name(poetry) -> str:
    return f"v{poetry['version']}"


def deploy() -> bool:
    poetry = load_pyproject()["tool"]["poetry"]

    repo = Repo(_project_root)
    tag_name = v_tag_name(poetry)
    repo.create_tag(tag_name)
    repo.remotes.origin.push(tag_name)

    return True


def poe_can_deploy() -> None:
    """Raise a SystemExit error if poe should not deploy the package."""
    repo = Repo(_project_root)
    poetry = load_pyproject()["tool"]["poetry"]

    if repo.is_dirty():
        msg = "Cannot deploy, current repo is dirty. Please commit all changes first."
        raise SystemExit(msg)

    tag_name = v_tag_name(poetry)
    if tag_name in repo.tags:
        msg = f"Cannot deploy, version {poetry['version']} has already been deployed."
        raise SystemExit(msg)


if __name__ == "__main__":
    try:
        poe_can_deploy()
        print("Poe would deploy the project!")
    except SystemExit as se:
        print(f"Poe would fail to deploy the project:\n{se}")
