import os

os.environ["ANNDATA_CACHE_KILL_ON_FAIL"] = os.environ.get("ANNDATA_CACHE_KILL_ON_FAIL", "true")
os.environ["ANNDATA_CACHE_FOLDER"] = os.environ.get("ANNDATA_CACHE_FOLDER", "/data/hbdx_ldap_local/anndata_cache/")
os.environ["ANNDATA_CACHE_PLASMA_LOCATION"] = os.environ.get("ANNDATA_CACHE_PLASMA_LOCATION", "/tmp/plasma-anndata")
os.environ["ANNDATA_CACHE_NAME_MAP_ID"] = os.environ.get("ANNDATA_CACHE_NAME_MAP_ID", "/id_name_map")


import logging

logger = logging.getLogger("anndata_cache")

try:
    import pyarrow.plasma

    _plasma_client = pyarrow.plasma.connect(os.environ["ANNDATA_CACHE_PLASMA_LOCATION"])
except OSError:
    from . import utils

    logger.error("Could not connect to plasma store. Is it running? Am stopping now.")
    utils.kill_app()

from ._other_types import *
from .crud import *
from .meta import *
