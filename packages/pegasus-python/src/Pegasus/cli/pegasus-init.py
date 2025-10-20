#!/usr/bin/env python3

import os
import sys

# PEGASUS_PYTHONPATH is set by the pegasus-python-wrapper script
peg_path = os.environ.get("PEGASUS_PYTHONPATH")
if peg_path:
    for p in reversed(peg_path.split(":")):
        if p not in sys.path:
            sys.path.insert(0, p)

from Pegasus.init import main

# pegasus_share_dir = (Path(os.environ["PEGASUS_HOME"]) / "share" / "pegasus").resolve()

# main(str(pegasus_share_dir))

main()
