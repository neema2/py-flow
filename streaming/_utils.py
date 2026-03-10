import platform

def _is_remote() -> bool:
    """Check if we are on ARM Linux (which requires remote Docker for Deephaven)."""
    sys_name = platform.system()
    machine = platform.machine().lower()
    is_arm = sys_name == "Linux" and any(arch in machine for arch in ("aarch64", "arm64", "armv8"))
    
    if not is_arm and sys_name == "Linux":
        # Extra check: if 'deephaven' is missing and it's Linux, we might be on a mystery ARM
        try:
            import deephaven
        except ImportError:
            # If we are on Linux and 'deephaven' (the x86 server) is missing, 
            # assume we need remote mode.
            return True
    return is_arm
