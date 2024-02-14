import os
from datetime import datetime
from pathlib import Path
from typing import Union

import anndata
import pandas as pd
import yaml

from . import logger
from .crud import contains, get, put
from .types import CacheLocation, Key

__all__ = ["metadata", "indices", "obs", "var", "X"]


def read_anndata(filepath: str):
    filepath = Path(filepath)
    logger.info(f"Loading {filepath} from HDD.")

    if not filepath.exists():
        raise FileNotFoundError(filepath)

    ad: anndata.AnnData = anndata.read_h5ad(filepath)
    st_mtime = datetime.fromtimestamp(filepath.stat().st_mtime).strftime("%Y-%m-%d")

    obs: pd.DataFrame = ad.obs
    obs_categoricals = obs.select_dtypes("category").astype(str)
    obs[obs_categoricals.columns] = obs_categoricals

    var: pd.DataFrame = ad.var
    var_categoricals = var.select_dtypes("category").astype(str)
    var[var_categoricals.columns] = var_categoricals

    X: pd.DataFrame = pd.DataFrame(ad.X, columns=ad.var_names, index=ad.obs_names)

    project = filepath.parent.name
    filename = filepath.stem

    metadata = {
        "id": str(filepath),
        "project": project,
        "filename": filename,
        "samples": ad.n_obs,
        "features": ad.n_vars,
        "sample annotations": len(ad.obs.columns),
        "feature annotations": len(ad.var.columns),
        "last modified": ad.uns.get("last_modified", st_mtime).split(" ")[0],
        "release notes": ad.uns.get("release_notes", ""),
        "version": ad.uns.get("version", str(filepath).split("-")[-1]),
        "log-base": int(ad.uns.get("log1p", {"base": 0}).get("base", 0)),
    }
    indices = {
        "samples": ad.obs_names.tolist(),
        "sample annotations": ad.obs.columns.tolist(),
        "feature annotations": ad.var.columns.tolist(),
    }
    for column in ["Lab_Multiplexing_pool_ID", "ProjectName", "Sample_Type", "Lab_Library_Protocol", "Diagnosis_Group"]:
        if column in ad.obs:
            metadata[column] = ad.obs[column].nunique()
            indices[column] = ad.obs[column].unique().tolist()
    for column in ["Sample_Group"]:
        if column in ad.obs:
            for k, v in ad.obs[column].value_counts().to_dict().items():
                if v > 10:
                    metadata[f"{column} {k}"] = v

    return {"X": X, "obs": obs, "var": var, "metadata": metadata, "indices": indices}


def __cool_anndata(key: Key, ad):
    logger.info(f"Cooling {key} to disk: {list(ad.keys())}.")
    cache_dir = Path(os.environ["ANNDATA_CACHE_FOLDER"])
    (cache_dir / key.name).mkdir(parents=True, exist_ok=True)

    if not (cache_dir / key.name / "obs.h5").exists():
        ad["obs"].to_hdf(cache_dir / key.name / "obs.h5", "data", mode="w")
    if not (cache_dir / key.name / "var.h5").exists():
        ad["var"].to_hdf(cache_dir / key.name / "var.h5", "data", mode="w")
    if not (cache_dir / key.name / "X.h5").exists():
        try:
            ad["X"].to_hdf(cache_dir / key.name / "X.h5", "data", mode="w", format="fixed")
        except ValueError:
            ad["X"].to_hdf(cache_dir / key.name / "X.h5", "data", mode="w", format="table")
    if not (cache_dir / key.name / "metadata.yaml").exists():
        with open(cache_dir / key.name / "metadata.yaml", "w") as fp:
            yaml.dump(ad["metadata"], fp)
    if not (cache_dir / key.name / "indices.yaml").exists():
        with open(cache_dir / key.name / "indices.yaml", "w") as fp:
            yaml.dump(ad["indices"], fp)


def __thaw_anndata(key: Key, members: list[str]):
    logger.debug(f"Thawing {key} from plasma store {members}.")
    cache_dir = Path(os.environ["ANNDATA_CACHE_FOLDER"])
    ad = {}

    if "obs" in members:
        keyObs = key.with_suffix("/obs.h5")
        ad["obs"] = pd.read_hdf(cache_dir / keyObs.name)

    if "var" in members:
        keyVar = key.with_suffix("/var.h5")
        ad["var"] = pd.read_hdf(cache_dir / keyVar.name)

    if "X" in members:
        keyX = key.with_suffix("/X.h5")
        ad["X"] = pd.read_hdf(cache_dir / keyX.name)

    if "metadata" in members:
        keyMeta = key.with_suffix("/metadata.yaml")
        with open(cache_dir / keyMeta.name, "r") as fp:
            ad["metadata"] = yaml.load(fp, Loader=yaml.FullLoader)

    if "indices" in members:
        keyIndices = key.with_suffix("/indices.yaml")
        with open(cache_dir / keyIndices.name, "r") as fp:
            ad["indices"] = yaml.load(fp, Loader=yaml.FullLoader)

    return ad


def __heat_anndata(key: Key, ad):
    logger.info(f"Heating from {key} to plasma store: {list(ad.keys())}.")
    if "obs" in ad:
        keyObs = key.with_suffix("/obs.h5")
        put(ad["obs"], keyObs, overwrite=False)

    if "var" in ad:
        keyVar = key.with_suffix("/var.h5")
        put(ad["var"], keyVar, overwrite=False)

    if "X" in ad:
        keyX = key.with_suffix("/X.h5")
        keyVar = key.with_suffix("/var_names")
        keyObs = key.with_suffix("/obs_names")
        put(ad["X"].values, keyX, overwrite=False)
        put(ad["X"].columns.tolist(), keyVar, overwrite=False)
        put(ad["X"].index.tolist(), keyObs, overwrite=False)

    if "metadata" in ad:
        keyMeta = key.with_suffix("/metadata.yaml")
        put(ad["metadata"], keyMeta, overwrite=False)

    if "indices" in ad:
        keyInd = key.with_suffix("/indices.yaml")
        put(ad["indices"], keyInd, overwrite=False)


def cache_anndata(key: Key, heatup: Union[list[str], bool]):
    cache_dir = Path(os.environ["ANNDATA_CACHE_FOLDER"])

    cold_files = {"obs": "obs.h5", "var": "var.h5", "X": "X.h5", "metadata": "metadata.yaml", "indices": "indices.yaml"}
    cold_files = {k: cache_dir / key.name / v for k, v in cold_files.items()}

    if heatup is True:
        heatup = list(cold_files.keys())
    heatup = set(heatup)

    if key.location == CacheLocation.HDD or not all([f.exists() for f in cold_files.values()]):
        # We need a full-reload if its a HDD file or if any of the cold files are missing
        if key.location == CacheLocation.HDD:
            filepath = key.name
        else:
            filepath = Path(os.environ["ANNDATA_CACHE_FOLDER"]) / (key.name + ".h5ad")
        ad = read_anndata(filepath)

        if key.location == CacheLocation.CACHED:
            __cool_anndata(key, ad)
    else:
        # the file is in the cache, so we can load it from there
        ad = __thaw_anndata(key, heatup)

    # Only heatup the requested members
    __heat_anndata(key, {k: v for k, v in ad.items() if k in heatup})


def metadata(key: Key) -> dict:
    """
    Loads the metadata for an AnnData from the cache or from disk.

    Parameters
    ----------
    key : Key
        The key of the AnnData.

    Returns
    -------
    dict
        The metadata as a dict.
    """
    key = Key(key)
    keyMeta = key.with_suffix("/metadata.yaml")

    if key.location == CacheLocation.HDD:
        if not contains(keyMeta):
            cache_anndata(key, True)
        metadata = get(keyMeta, "object")

    else:
        cache_dir = Path(os.environ["ANNDATA_CACHE_FOLDER"])
        if not (cache_dir / keyMeta.name).exists():
            cache_anndata(key, [])
        metadata = yaml.load((cache_dir / keyMeta.name).read_text(), Loader=yaml.FullLoader)
    return metadata


def indices(key: Key) -> dict:
    """
    Loads the indices for an AnnData from the cache or from disk.

    Parameters
    ----------
    key : Key
        The key of the AnnData.

    Returns
    -------
    dict
        The indices as a dict.
    """
    key = Key(key)
    keyMeta = key.with_suffix("/indices.yaml")

    if key.location == CacheLocation.HDD:
        if not contains(keyMeta):
            cache_anndata(key, True)
        indices = get(keyMeta, "object")
    else:
        cache_dir = Path(os.environ["ANNDATA_CACHE_FOLDER"])
        if not (cache_dir / keyMeta.name).exists():
            cache_anndata(key, [])
        indices = yaml.load((cache_dir / keyMeta.name).read_text(), Loader=yaml.FullLoader)
    return indices


def obs(key: Key) -> pd.DataFrame:
    """
    Loads the obs matrix for an AnnData from the cache or from disk.

    Parameters
    ----------
    key : Key
        The key of the AnnData.

    Returns
    -------
    pd.DataFrame
        The obs matrix as a pandas dataframe.
    """
    key = Key(key)
    keyObs = key.with_suffix("/obs.h5")

    if not contains(keyObs):
        cache_anndata(key, ["obs"] if key.location == CacheLocation.CACHED else True)
    return get(keyObs, "pandas")


def var(key: Key) -> pd.DataFrame:
    """
    Loads the var matrix for an AnnData from the cache or from disk.

    Parameters
    ----------
    key : Key
        The key of the AnnData.

    Returns
    -------
    pd.DataFrame
        The var matrix as a pandas dataframe.
    """
    key = Key(key)
    keyVar = key.with_suffix("/var.h5")

    if not contains(keyVar):
        cache_anndata(key, ["var"] if key.location == CacheLocation.CACHED else True)
    return get(keyVar, "pandas")


def X(key: Key) -> pd.DataFrame:
    """
    Loads the X matrix for an AnnData from the cache or from disk.

    Parameters
    ----------
    key : Key
        The key of the AnnData.

    Returns
    -------
    pd.DataFrame
        The X matrix as a pandas dataframe.
    """
    key = Key(key)

    keyX = key.with_suffix("/X.h5")
    keyVar = key.with_suffix("/var_names")
    keyObs = key.with_suffix("/obs_names")

    if not contains(keyX, keyVar, keyObs):
        cache_anndata(key, ["X"] if key.location == CacheLocation.CACHED else True)

    x = get(keyX, "numpy")
    var_names: list[str] = get(keyVar, "object")
    obs_names: list[str] = get(keyObs, "object")
    x = pd.DataFrame(x, columns=var_names, index=obs_names)
    return x
