#!/usr/bin/env python3

import os
from pathlib import Path

from Pegasus.init import main

pegasus_share_dir = (Path(os.environ["PEGASUS_HOME"]) / "share" / "pegasus").resolve()

main(str(pegasus_share_dir))
