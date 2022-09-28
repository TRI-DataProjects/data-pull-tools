import os
import venv
from subprocess import run
from pathlib import Path

root = Path(__file__).parent.resolve()
venv_dir = (root / ".venv").resolve()

if os.name == "nt":
    # windows
    venv_pip = (venv_dir / "Scripts" / "pip.exe").resolve()
    venv_py = (venv_dir / "Scripts" / "python.exe").resolve()
    pass
else:
    # posix
    venv_pip = (venv_dir / "bin" / "pip").resolve()
    venv_py = (venv_dir / "bin" / "python").resolve()


req = (root / "requirements.txt").resolve()

# Does venv exist?
if not os.path.exists(venv_dir):
    # No, create it
    venv.create(venv_dir, with_pip=True)
    run([venv_py, "-m", "pip", "install", "--upgrade", "pip"])
    run([venv_pip, "install", "wheel"])
    run([venv_pip, "install", "-r", req])
