import os
from typing import Any

import pyarrow.plasma as plasma

from . import _plasma_client, logger, utils
from .types import Key

__all__ = ["current_usage", "list_objects"]

_id_name_map_key = Key(os.environ["ANNDATA_CACHE_NAME_MAP_ID"])


def current_usage() -> dict[str, float]:
    """Returns a namespace with information about the Plasma store.

    Returns
    -------
    dict
        Currently contains the following keys:
        - capacity (Total capacity of the store in Gigabytes)
        - used_memory (Amount of the store in usage)
        - number_of_objects (Number of objects in the store)
        - utilization (Percentage of the store in use)
    """
    try:
        objects: dict[plasma.ObjectID, dict[str, Any]] = _plasma_client.list()
        capacity = _plasma_client.store_capacity() / 10**9  # in GB
    except OSError as e:
        logger.error(
            f"Plasma failed during execution of 'current_usage()'\n{e}\n\nNormally this is because the Plasma Daemon was restarted. We will restart the dashboard."
        )
        utils.kill_app()
    number_of_objects = len(objects)

    used_memory = sum([o["data_size"] for o in objects.values()]) / 10**9  # in GB

    utilization = used_memory / capacity

    return dict(capacity=capacity, used_memory=used_memory, number_of_objects=number_of_objects, utilization=utilization)


def list_objects() -> dict[plasma.ObjectID, dict[str, Any]]:
    """Lists all objects in the plasma store.
    Tries to get the name of the object from the internal id_name_map.

    Returns
    -------
    dict[plasma.ObjectID, dict[str, Any]
        A dict with the object ids as keys and a dict with the following keys as values:
        - data_size (size of the object in bytes)
        - metadata_size (size of the metadata in bytes)
        - ref_count (number of references to the object)
        - name (name of the object, if known)
    """
    id_name_map = dict()
    if _plasma_client.contains(_id_name_map_key.id):
        id_name_map = _plasma_client.get(_id_name_map_key.id)
        id_name_map[_id_name_map_key.id.binary().hex()] = _id_name_map_key.name + " (internal)"

    return {objid: {**obj, "name": id_name_map.get(objid.binary().hex())} for objid, obj in _plasma_client.list().items()}
