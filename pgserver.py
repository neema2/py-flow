# pgserver.py (Shim in project root)
import sys

try:
    import pixeltable_pgserver
    from pixeltable_pgserver import *
    # Crucial: Export __file__ so the app can find the extension control files
    __file__ = pixeltable_pgserver.__file__
except ImportError:
    # Fallback to standard pgserver if it exists (e.g., on x86_64)
    import pgserver as _pgserver
    from pgserver import *
    __file__ = _pgserver.__file__
