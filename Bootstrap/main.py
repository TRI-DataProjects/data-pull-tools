import os
import sys
from pathlib import Path

import pandas as pd
from Tools.CachedExcelReader import CachedExcelReader

input_name = "input_name"

root = Path(__file__).parent.absolute()
input_root = root / "Input" / input_name
output_root = root / "Output" / input_name
if not os.path.exists(output_root):
    os.mkdir(output_root)
reader = CachedExcelReader(input_root)


def __main__() -> None:
    print("Hello, World!")


if __name__ == "__main__":
    debug = True
    # Run fast and loose, let errors fly
    if debug:
        __main__()
    # Run "safely", always exit "gracefully"
    else:
        exit_code = 1
        try:
            __main__()
            exit_code = 0
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
        sys.exit(exit_code)
