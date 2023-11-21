import signal
import os

__all__ = ["kill_app"]

def kill_app() -> None:
    """
    Kills the app by sending a SIGKILL to the current process.
    You can disable this behavior by setting the environment variable ANNDATA_CACHE_KILL_ON_FAIL to "false"
    """
    if os.environ["ANNDATA_CACHE_KILL_ON_FAIL"] == "false":
        return
    pid = os.getpid()
    os.kill(pid, signal.SIGKILL)
