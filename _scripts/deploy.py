import os
import shutil
from pathlib import Path
from typing import Any

from git.repo import Repo

from data_pull_tools.toml_utils import CachedTOMLReader

_reader = CachedTOMLReader()

_project_root = Path(__file__).parent.parent
_dist_dir = _project_root / "dist"


def get_dest_dir() -> Path:
    pyproject = load_pyproject()
    sys_name = "win" if os.name == "nt" else "posix"
    key_name = f"{sys_name}_deploy_paths"
    paths = pyproject["tool"]["_scripts"][key_name]

    dest_dir: Path | None = None

    # Stop at first valid deployment path
    for path in [Path(x) for x in paths]:
        if path.exists() and path.is_dir():
            dest_dir = path
            break

    if dest_dir is None:
        raise FileNotFoundError(
            f"""Could not find a suitable deployment path, are you connected to the network?
Please check [tool._scripts] {key_name} in {_project_root / 'pyproject.toml'}"""
        )

    return dest_dir / pyproject["tool"]["poetry"]["name"]


def load_pyproject() -> dict[str, Any]:
    return _reader.load(_project_root / "pyproject.toml")


def v_tag_name(poetry) -> str:
    return f"v{poetry['version']}"


def already_deployed() -> bool:
    dest = get_dest_dir()
    poetry = load_pyproject()["tool"]["poetry"]
    pattern = f"{poetry['name']}-{poetry['version']}*"

    # If the destination folder doesn't exist there hasn't been a deployment yet
    if not dest.exists():
        return False

    # Has there already been a deployment of files?
    return len(list(dest.glob(pattern))) != 0


def deploy() -> bool:
    if already_deployed():
        return False

    dest = get_dest_dir()
    poetry = load_pyproject()["tool"]["poetry"]
    pattern = f"{poetry['name']}-{poetry['version']}*"

    if not dest.exists():
        dest.mkdir()

    for path in _dist_dir.glob(pattern):
        shutil.copy2(path, dest)

    repo = Repo(_project_root)
    tag_name = v_tag_name(poetry)
    repo.create_tag(tag_name)
    repo.remotes.origin.push(tag_name)

    return True


def poe_can_deploy() -> None:
    """Raise a SystemExit error if poe should not deploy the package"""

    repo = Repo(_project_root)
    poetry = load_pyproject()["tool"]["poetry"]

    if repo.is_dirty():
        raise SystemExit(
            "Cannot deploy, current repo is dirty. Please commit all changes first."
        )

    tag_name = v_tag_name(poetry)
    if tag_name in repo.tags:
        raise SystemExit(f"Cannot deploy, tag alreadys exists with name '{tag_name}'")

    if already_deployed():
        raise SystemExit(
            f"Cannot deploy, version {poetry['version']} has already been deployed."
        )


if __name__ == "__main__":
    try:
        poe_can_deploy()
        print("Poe would deploy the project!")
    except SystemExit as se:
        print(f"Poe would fail to deploy the project:\n{se}")
