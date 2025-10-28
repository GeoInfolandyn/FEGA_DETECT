"""
Zonal time-series extraction for Sentinel-2 on Planetary Computer (pythonic, structured).

- Computes NDVI, AR y AS1 para cada geometría.
- Devuelve un GeoDataFrame con una columna por índice que contiene **arrays** (listas) con la serie temporal.
- Incluye timestamps ISO compartidos por geometría en la columna `time_stamps` (array de str ISO8601).
- Opcionalmente exporta a CSV (arrays serializados como JSON) y/o Parquet/GeoPackage.

Requisitos (sugeridos):
    geopandas, xarray, rioxarray, odc-stac, pystac-client, planetary-computer,
    numpy, pandas, scipy, shapely, joblib, tqdm

Uso CLI básico:
    python zonal_timeseries_refactor.py \
        --shp /ruta/parcelas.shp \
        --crs-epsg 32630 \
        --start 2017-01-01 --end 2024-12-31 \
        --indices NDVI AR AS1 \
        --resolution 10 \
        --out-parquet /ruta/salida.parquet

Nota: Para CSV, los arrays se guardan como JSON. Parquet/GeoPackage conservan listas nativas.
"""
from __future__ import annotations

import json
import warnings
from dataclasses import dataclass
from typing import Dict, List, Tuple, Iterable, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
import rioxarray as rio  # noqa: F401 (side-effects)
from shapely.geometry import mapping
from pystac_client import Client
import planetary_computer
from odc.stac import load
from scipy.signal import savgol_filter
from tqdm import tqdm

warnings.filterwarnings("ignore")

# ----------------------------- Configuración ----------------------------- #

INDICES_CONFIG: Dict[str, Dict[str, List[str]]] = {
    "NDVI": {"bands": ["B08", "B04"]},
    "AR": {"bands": ["B03", "B04", "B08"]},
    "AS1": {"bands": ["B08", "B11", "B12"]},
}

# Bandas extra necesarias (SCL para máscara de nubes)
EXTRA_BANDS = ["SCL"]

# ----------------------------- Utilidades ------------------------------- #

def _union_bands(indices: Iterable[str]) -> List[str]:
    bands = set(EXTRA_BANDS)
    for idx in indices:
        info = INDICES_CONFIG.get(idx.upper())
        if not info:
            raise ValueError(f"Índice no reconocido: {idx}")
        bands.update(info["bands"])
    return sorted(bands)


def _cloud_mask_s2(stack: xr.Dataset) -> xr.Dataset:
    """Aplica máscara de nubes usando SCL (Sentinel-2 L2A)."""
    if "SCL" not in stack:
        return stack
    valid = (
        (stack["SCL"] != 0)
        & (stack["SCL"] != 1)
        & (stack["SCL"] != 2)
        & (stack["SCL"] != 3)
        & (stack["SCL"] != 8)
        & (stack["SCL"] != 9)
        & (stack["SCL"] != 10)
        & (stack["SCL"] != 11)
    )
    out = stack.where(valid)
    out.attrs = stack.attrs
    return out


def _preprocess_s2(ds: xr.Dataset, fecha_corte: str = "2022-01-24") -> xr.Dataset:
    """Ajuste de offset posterior a la fecha de corte + máscara de nubes."""
    if "time" not in ds.coords:
        return ds
    ds2 = ds.copy()
    mask_date = ds2["time"] >= np.datetime64(fecha_corte)
    for b in ds2.data_vars:
        if b != "SCL":
            ds2[b] = xr.where(mask_date, ds2[b] - 1000, ds2[b])
            ds2[b] = ds2[b].clip(min=0)
    ds2 = _cloud_mask_s2(ds2)
    return ds2

# ------------------------ Cálculo de índices ---------------------------- #

def _calc_ndvi(ds: xr.Dataset) -> xr.DataArray:
    ndvi = (ds["B08"] - ds["B04"]) / (ds["B08"] + ds["B04"])
    return (ndvi * 10000).astype("int16").rename("NDVI")


def _calc_ar(ds: xr.Dataset) -> xr.DataArray:
    b3, b4, b8 = ds["B03"], ds["B04"], ds["B08"]
    a = np.sqrt((664 - 560) ** 2 + (b4 - b3) ** 2)
    b = np.sqrt((835 - 664) ** 2 + (b8 - b4) ** 2)
    c = np.sqrt((835 - 560) ** 2 + (b8 - b3) ** 2)
    cosv = (a ** 2 + b ** 2 - c ** 2) / (2 * a * b)
    cosv = cosv.clip(-1.0, 1.0)
    ar = np.arccos(cosv) * 10000
    return ar.astype("int16").rename("AR")


def _calc_as1(ds: xr.Dataset) -> xr.DataArray:
    b8, b11, b12 = ds["B08"], ds["B11"], ds["B12"]
    a = np.sqrt((1613.7 - 835) ** 2 + (b11 - b8) ** 2)
    b = np.sqrt((1613.7 - 2202) ** 2 + (b11 - b12) ** 2)
    c = np.sqrt((2202 - 835) ** 2 + (b12 - b8) ** 2)
    cosv = (a ** 2 + b ** 2 - c ** 2) / (2 * a * b)
    cosv = cosv.clip(-1.0, 1.0)
    as1 = np.arccos(cosv) * 10000
    return as1.astype("int16").rename("AS1")


CALC_FUN: Dict[str, callable] = {
    "NDVI": _calc_ndvi,
    "AR": _calc_ar,
    "AS1": _calc_as1,
}

# -------------------- Serie temporal (media zonal) ---------------------- #

@dataclass
class SeriesParams:
    resample_rule: str = "5D"
    sg_window: int = 5
    sg_polyorder: int = 2


def _mean_series(
    arr: xr.DataArray,
    params: SeriesParams,
) -> Tuple[pd.DatetimeIndex, np.ndarray]:
    """Media espacial -> resample -> interpolación -> suavizado.

    Devuelve fechas (DatetimeIndex) y valores (np.ndarray int16/uint16).
    """
    # Media espacial
    ts = arr.mean(dim=["x", "y"]).to_series()
    # Resampleo y media en ventana
    ts = ts.resample(params.resample_rule).mean()
    # Interpolación lineal para gaps
    ts = ts.interpolate(method="time")
    # Suavizado Savitzky-Golay (si hay suficientes puntos)
    values = ts.values.astype(float)
    if len(values) >= params.sg_window:
        values = savgol_filter(values, window_length=params.sg_window, polyorder=params.sg_polyorder)
    # Escalado final a enteros si procede
    if np.issubdtype(arr.dtype, np.integer):
        values = np.rint(values).astype(arr.dtype)
    time_idx = ts.index
    return time_idx, values

# ---------------------- Carga y recorte STAC ---------------------------- #

def _load_s2(
    bbox: List[float],
    geopolygon,
    crs_epsg: int,
    start: str,
    end: str,
    bands: List[str],
    resolution: int = 10,
) -> xr.Dataset:
    client = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    search = client.search(
        collections=["sentinel-2-l2a"],
        bbox=bbox,
        datetime=f"{start}/{end}",
    )
    ds = load(
        search.items(),
        bands=bands,
        crs=f"EPSG:{crs_epsg}",
        resolution=resolution,
        geopolygon=geopolygon,
        groupby="solar_day",
        patch_url=planetary_computer.sign,
        fail_on_error=False,
        chunks={"x": 2048, "y": 2048},
    )
    return ds

# ----------------------- Pipeline por geometría ------------------------- #

def process_geometry(
    geom,
    indices: List[str],
    start: str,
    end: str,
    crs_data_epsg: int = 32630,
    resolution: int = 10,
    fecha_corte: str = "2022-01-24",
    params: Optional[SeriesParams] = None,
) -> Dict[str, List]:
    """Procesa una geometría y devuelve dict con arrays por índice + timestamps.

    Retorna un diccionario, p.ej. {
        'time_stamps': ["2020-01-01", ...],
        'NDVI_ts': [..], 'AR_ts': [..], 'AS1_ts': [..]
    }
    """
    params = params or SeriesParams()
    idxs = [i.upper() for i in indices]
    bands = _union_bands(idxs)

    # Asegurar CRS de trabajo y bbox en WGS84 para la búsqueda
    roi = gpd.GeoSeries([geom]).set_crs(epsg=crs_data_epsg)
    bbox = roi.to_crs(epsg=4326).total_bounds.tolist()
    geopolygon = roi.geometry

    ds = _load_s2(bbox, geopolygon, crs_data_epsg, start, end, bands, resolution)
    ds = _preprocess_s2(ds, fecha_corte)

    out: Dict[str, List] = {}
    time_ref: Optional[pd.DatetimeIndex] = None

    for idx in idxs:
        # Subselección de bandas necesarias para el índice
        needed = INDICES_CONFIG[idx]["bands"] + ["SCL"]
        dsi = ds[needed]
        # Cálculo del índice
        arr = CALC_FUN[idx](dsi)
        # Serie temporal
        t, v = _mean_series(arr, params)
        # Guardar arrays (listas puras para buen soporte IO)
        out[f"{idx}_ts"] = v.tolist()
        if time_ref is None:
            time_ref = t

    # timestamps ISO (sin zona horaria) para cada paso
    assert time_ref is not None
    out["time_stamps"] = [pd.Timestamp(t).strftime("%Y%m%d") for t in time_ref]
    return out

# -------------------- Procesado de un GeoDataFrame ---------------------- #

def process_gdf(
    gdf: gpd.GeoDataFrame,
    indices: List[str] = ("NDVI", "AR", "AS1"),
    start: str = "2017-01-01",
    end: str = "2024-12-31",
    crs_data_epsg: int = 32630,
    resolution: int = 10,
    fecha_corte: str = "2022-01-24",
    params: Optional[SeriesParams] = None,
    progress: bool = True,
) -> pd.DataFrame:
    """
    Devuelve un DataFrame en formato largo listo para process_series/opcion2.
    Rellena huecos con ceros y asegura que todas las series tengan la misma longitud.
    """
    params = params or SeriesParams()
    idxs = [i.upper() for i in indices]

    # Asegurar CRS de datos
    if gdf.crs is None or gdf.crs.to_epsg() != crs_data_epsg:
        gdf_proc = gdf.to_crs(epsg=crs_data_epsg)
    else:
        gdf_proc = gdf.copy()

    registros: List[Dict] = []
    iterator = tqdm(
        gdf_proc.itertuples(index=False),
        total=len(gdf_proc),
        disable=not progress,
        desc="Parcelas"
    )


    for row in iterator:
        geom = row.geometry
        series_dict = process_geometry(
            geom=geom,
            indices=idxs,
            start=start,
            end=end,
            crs_data_epsg=crs_data_epsg,
            resolution=resolution,
            fecha_corte=fecha_corte,
            params=params,
        )

        # Tomamos las fechas realmente disponibles
        if not series_dict["time_stamps"]:
            continue  # saltar si no hay datos

        start_real = min(series_dict["time_stamps"])
        end_real = max(series_dict["time_stamps"])
        all_dates_5d = pd.date_range(start=start_real, end=end_real, freq="5D").to_pydatetime().tolist()
        # Asegurar que cada serie tenga la misma longitud que all_dates_5d
        for idx in idxs:
            key = f"{idx}_ts"
            serie = series_dict.get(key, [])
            if len(serie) < len(all_dates_5d):
                serie = list(serie) + [0] * (len(all_dates_5d) - len(serie))
            series_dict[key] = serie

        # Expandir en filas (formato largo)
        attrs = {k: getattr(row, k) for k in gdf_proc.columns if k != "geometry"}
        for i, fecha in enumerate(all_dates_5d):
            registro = attrs.copy()
            registro["fecha"] = fecha
            for idx in idxs:
                registro[idx] = series_dict[f"{idx}_ts"][i]
            registros.append(registro)

    out_df = pd.DataFrame(registros)
    return out_df


# ----------------------------- Exportadores ----------------------------- #

def to_csv_arrays_as_json(df: pd.DataFrame, path: str) -> None:
    """Exporta a CSV serializando listas en JSON para columnas *_ts y `time_stamps`."""
    def _serialize(val):
        if isinstance(val, list):
            return json.dumps(val, ensure_ascii=False)
        return val

    df2 = df.copy()
    cols = [c for c in df2.columns if c.endswith("_ts") or c == "time_stamps"]
    for c in cols:
        df2[c] = df2[c].map(_serialize)
    df2.to_csv(path, index=False)


def to_parquet(df: pd.DataFrame, path: str) -> None:
    df.to_parquet(path, index=False)


def to_geopackage(gdf: gpd.GeoDataFrame, path: str, layer: str = "series") -> None:
    gdf.to_file(path, layer=layer, driver="GPKG")


