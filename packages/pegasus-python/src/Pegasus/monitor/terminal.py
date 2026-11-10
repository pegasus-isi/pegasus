"""Terminal-safe presentation of untrusted monitor strings."""

import re

# C0/C1 controls include ESC, CSI and OSC; printable Unicode is preserved.
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f-\x9f]")


def sanitize_display(value: str) -> str:
    """Replace terminal controls with spaces, without changing stored data."""
    return _CONTROL_CHARS.sub(" ", value)
