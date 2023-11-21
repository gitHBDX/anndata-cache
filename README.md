# `anndata-cache` 

<img src="https://arrow.apache.org/docs/_static/arrow.png" alt="pyarrow logo" height="50"/>
<svg xmlns="http://www.w3.org/2000/svg" class="icon icon-tabler icon-tabler-plus" width="50" height="50" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" fill="none" stroke-linecap="round" stroke-linejoin="round"><path stroke="none" d="M0 0h24v24H0z" fill="none"/><path d="M12 5l0 14" /><path d="M5 12l14 0" /></svg>
<img src="https://raw.githubusercontent.com/scverse/anndata/main/docs/_static/img/anndata_schema.svg" alt="pyarrow logo" height="50"/>

A shared-process in-memory store/cache for AnnData objects and their parts.

**_Note_: The Plasma in-memory store of pyArrow is deprecated. Therefore this project is stuck in a limbo. But as of now it works.**

## Installation

```bash
pip install git+https://github.com/gitHBDX/anndata-cache.git
```

You have to start the `plasma_store` on your own beforehand. It's best to make the plasma cache public after creation so the library cam work multi-process.

```bash
plasma_store -m {size_of_store} -s /tmp/plasma-dashboards
chmod 775 /tmp/plasma-dashboards
```

Alternatively, you can manage the `plasma_store` with `systemd`. See the template Service file in `./systemd`.

## Configuration

You can configure the Store with environment variables:

- `ANNDATA_CACHE_KILL_ON_FAIL` (default: `true`): Whether to kill the whole app on an plasma error. Useful, if an app was was started after the plasma store was started and should auto-restart.
- `ANNDATA_CACHE_FOLDER`: Location on Disk whether to keep the cold cache
- `ANNDATA_CACHE_PLASMA_LOCATION` (default: `/tmp/plasma-anndata`): Location in-memory where to keep the hot cache

## Usage

The cache consists of a cold on-drive and a hot in-memory cache. When objects are requested they're put into the hot cache first-in-first-out, but also saved as a fast to retrieve partilized version in the cold cache.

```python
# file1.py
import pandas as pd
import anndata_cache as cache

X: pd.DataFrame = cache.X("/path/to/some/anndata_file.h5ad")
# will load the anndata file and put _all_ parts of it in the cache for now but here return only the ad.X expression matrix as a DataFrame
```

```python
# file2.py
import pandas as pd
import anndata_cache as cache

obs: pd.DataFrame = cache.obs("/path/to/some/anndata_file.h5ad")
# now because the whole anndata was already cached by the other process, in this evocation the call to obs is _essentially_ free. If in the mean time the hot cache ran full, this obs is not loaded from the orignal h5ad file but from the cold cache.
```

-----

<p>Developed @</p>
<img src="https://www.hummingbird-diagnostics.com/application/files/4214/6893/9202/logo.png" alt="Hummingbid Diagnostics logo" width="200"/>
