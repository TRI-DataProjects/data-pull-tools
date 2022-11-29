import os
import venv
from asyncio import subprocess
from pathlib import Path
from subprocess import check_output, run

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


req_file = (root / "requirements.txt").resolve()

# Does venv exist?
if not os.path.exists(venv_dir):
    # No, create it
    venv.create(venv_dir, with_pip=True)
    run([venv_py, "-m", "pip", "install", "--upgrade", "pip"])
    run([venv_pip, "install", "wheel"])

run([venv_pip, "install", "-r", req_file])


# # Upgrade all packages not in a specified version range
# # This is a kludge and may break things!
# reqs = check_output([venv_py, "-m", "pip", "freeze"])
# req_list = reqs.decode().splitlines()
# for req in req_list:
#     pkg_name = req.split("==")[0]
#     # If the split does nothing do not upgrade this req
#     if pkg_name != req:
#         run([venv_pip, "install", "--upgrade", pkg_name])


# # Freeze requirements
# reqs = check_output([venv_py, "-m", "pip", "freeze"])
# with open(req_file, "wb") as req_file:
#     req_file.write(reqs)

print("Setup complete!")
