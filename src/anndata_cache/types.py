import hashlib
import os
from enum import Enum
from pathlib import Path

import pyarrow.plasma as plasma


class CacheLocation(Enum):
    CACHED = "cached"
    HDD = "hdd"


class Key:
    """
    A Key is a unique identifier for an object in the cache. It is used to store and retrieve objects from the cache.

    Parameters
    ----------
    filename : str
        The filename of the object. This can be either a full path, or a relative path. If it is a relative path, it is assumed that the object is in the cache.
    """

    def __new__(cls, filename: str):
        if isinstance(filename, Key):
            return filename
        else:
            return super().__new__(cls)

    def __init__(self, filename: str):
        if id(self) == id(filename):
            return
        # NOTE: do not very much like this, have to distingish between full paths and paths inside the cache
        # right now, i check for 3 or less path components, to allwo for DATASET/DATASET-VERSION/obs.h5  e.g.
        if len(filename.strip("/").split("/")) <= 3:
            self.name = filename.replace(".h5ad", "").strip("/.")
            self.path = Path(os.environ["ANNDATA_DATA_FOLDER"]) / (self.name + ".h5ad")
            self.location = CacheLocation.CACHED
        else:
            self.name = str(Path(filename).absolute())
            self.path = Path(filename)
            self.location = CacheLocation.HDD

        self.id = Key.object_id_from_string(self.name)

    def __repr__(self) -> str:
        return f"Key({self.name}, {self.location})"

    def with_suffix(self, suffix: str) -> "Key":
        """
        Returns a new Key with the suffix appended to the name.

        Parameters
        ----------
        suffix : str
            The suffix to append to the name
        """
        return Key(self.name + suffix)

    @staticmethod
    def object_id_from_string(name: str) -> plasma.ObjectID:
        """Takes a string and returns a deterministic plasma ObjectID from it by hashing the string with shake.

        Parameters
        ----------
        name : str
            The string used as the object name.

        Returns
        -------
        plasma.ObjectID
            Deterministic object ID
        """
        return plasma.ObjectID(hashlib.shake_128(str.encode(name)).digest(20))
