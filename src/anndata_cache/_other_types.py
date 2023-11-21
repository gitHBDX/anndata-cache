import pandas as pd

from .crud import contains, get, put
from .types import Key

__all__ = ["csv"]


def csv(key: Key, **kwargs) -> pd.DataFrame:
    """
    Loads a csv file from the cache or from disk.

    Parameters
    ----------
    key : Key
        The key of the csv file.

    Returns
    -------
    pd.DataFrame
        The csv file as a pandas dataframe.
    """
    key = Key(key)

    if contains(key):
        dataframe = get(key, "pandas")
    else:
        dataframe = pd.read_csv(key.name, **kwargs)
        put(dataframe, key)

    return dataframe
