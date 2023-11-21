import os
from typing import Any, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow.plasma import PlasmaStoreFull

from . import _plasma_client, logger, utils
from .types import Key

__all__ = ["contains", "put", "get", "delete"]

_id_name_map_key = Key(os.environ["ANNDATA_CACHE_NAME_MAP_ID"])


def contains(*keys: Key) -> bool:
    """Checks whether the plasma store contains this object.

    Parameters
    ----------
    keys : Key
        The key object for the object.

    Returns
    -------
    bool
        yes,no
    """
    logger.debug(f"Checking if {keys} are in plasma store.")
    results = []
    for key in keys:
        key = Key(key)
        try:
            results.append(_plasma_client.contains(key.id))
        except OSError as e:
            logger.error(
                f"Plasma failed during execution of 'contains({key})'\n{e}\n\nNormally this is because the Plasma Daemon was restarted. We will restart the dashboard."
            )
            utils.kill_app()
    return all(results)


def put(obj: Union[pd.DataFrame, np.ndarray], Any, key: Key, overwrite: bool = True) -> None:
    """Puts the given object into the plasma store using the supplied name.

    Parameters
    ----------
    obj : Union[pd.DataFrame, np.ndarray, Any]
        - `pandas.DataFrame`: use the dedicated pyarrow RecordBatch writer.
        - `numpy.ndarray`: use the dedicated pyarrow Tensor writer
        - _anything else_ puts the object pickled in (slow!).
    key: Key
        The ID to write to. If of type `plasma.ObjectID` used directly, if is
        `str` then generate deterministic object id from that and if None generates
        a random object ID, by default None
    overwrite : bool, optional
        Whether to overwrite the object if it already exists, by default True
    """
    key = Key(key)

    id_name_map = dict()
    if _plasma_client.contains(_id_name_map_key.id):
        id_name_map = _plasma_client.get(_id_name_map_key.id)
        _plasma_client.delete([_id_name_map_key.id])
    id_name_map[key.id.binary().hex()] = key.name
    _plasma_client.put(id_name_map, _id_name_map_key.id)

    try:
        if _plasma_client.contains(key.id):
            if not overwrite:
                return
            else:
                logger.debug(f"Object {key} already in plasma store. Deleting it first.")
                _plasma_client.delete([key.id])

        logger.debug(f"Putting {key} into plasma store.")
        if isinstance(obj, pd.DataFrame):
            record_batch = pa.RecordBatch.from_pandas(obj)

            mock_sink = pa.MockOutputStream()
            with pa.RecordBatchStreamWriter(mock_sink, record_batch.schema) as stream_writer:
                stream_writer.write_batch(record_batch)
            data_size = mock_sink.size()

            try:
                buf = _plasma_client.create(key.id, data_size)
            except pa._plasma.PlasmaObjectExists:
                print(key.id)
                raise
            stream = pa.FixedSizeBufferWriter(buf)
            with pa.RecordBatchStreamWriter(stream, record_batch.schema) as stream_writer:
                stream_writer.write_batch(record_batch)
            _plasma_client.seal(key.id)
        elif isinstance(obj, np.ndarray):
            tensor = pa.Tensor.from_numpy(obj)
            data_size = pa.ipc.get_tensor_size(tensor)

            buf = _plasma_client.create(key.id, data_size)
            stream = pa.FixedSizeBufferWriter(buf)
            pa.ipc.write_tensor(tensor, stream)
            _plasma_client.seal(key.id)
        else:
            _plasma_client.put(obj, key.id)
    except PlasmaStoreFull as e:
        msg = f"PlasmaStoreFull when putting {key}"
        logger.info(msg)
        raise PlasmaStoreFull(msg)
    except OSError as e:
        logger.error(
            f"Plasma failed during execution of 'put({key})'\n{e}\n\nNormally this is because the Plasma Daemon was restarted. We will restart the dashboard."
        )
        utils.kill_app()


def get(key: Key, dtype: str) -> Union[pd.DataFrame, np.ndarray, Any]:
    """Retrieves an object from the plasma store.

    Parameters
    ----------
    key : Key
        The object name to look for. If is `str` will calculate the deterministic
        object ID from it.
    dtype : str
        One of {"pandas", "numpy", "object"}. To know which reader to use to read
        the binary from the store, need to specify the object type.

    Returns
    -------
    Union[pd.DataFrame, np.ndarray, Any]
        The retrieved object

    Raises
    ------
    PreventUpdate
        if object id is None or empty
    """
    key = Key(key)
    assert dtype in {"pandas", "numpy", "object"}
    logger.debug(f"Getting {key} from plasma store.")

    try:
        if dtype == "pandas":
            [data] = _plasma_client.get_buffers([key.id])
            buffer = pa.BufferReader(data)
            reader = pa.RecordBatchStreamReader(buffer)
            record_batch = reader.read_next_batch()
            obj = record_batch.to_pandas()
        elif dtype == "numpy":
            [buf] = _plasma_client.get_buffers([key.id])

            reader = pa.BufferReader(buf)
            tensor = pa.ipc.read_tensor(reader)
            obj = tensor.to_numpy()
        else:
            obj = _plasma_client.get(key.id)
    except OSError as e:
        logger.error(
            f"Plasma failed during execution of 'get({key})'\n{e}\n\nNormally this is because the Plasma Daemon was restarted. We will restart the dashboard."
        )
        utils.kill_app()

    return obj


def delete(*keys: Key) -> None:
    """Deletes the object with given name from plasma. Does nothing if it doesn't
    exist.

    Parameters
    ----------
    key : Key
        The key object for the object.
    """
    for key in keys:
        key = Key(key)
        try:
            if _plasma_client.contains(key.id):
                _plasma_client.delete([key.id])
                logger.info(f"Deleted {key} from plasma store.")
        except OSError as e:
            logger.error(
                f"Plasma failed during execution of 'delete({key})'\n{e}\n\nNormally this is because the Plasma Daemon was restarted. We will restart the dashboard."
            )
            utils.kill_app()
