# import lib.marcadoresupm as upm
from test.context import lib
from lib.procesamiento.upm import marcadoresupm as upm
import ee
import geemap
import os
import time
import pandas as pd
from tqdm import tqdm
from google.cloud import storage
import google.auth
import requests
import csv
import json
from oauth2client.service_account import ServiceAccountCredentials
import re
import geopandas as gpd
from lib.descarga.zonal_timeseries_refactor import process_gdf, SeriesParams

def preparar_df_para_marcadores(df_wide, indices=['NDVI','AR','AS1']):
    df_temp = df_wide.copy()
    # columnas de series temporales
    series_cols = []
    for idx in indices:
        series_cols += [c for c in df_temp.columns if c.endswith(f"T_{idx}")]
    # mantener solo ID_STATS + columnas de series
    cols_to_keep = ['ID_STATS'] + series_cols
    return df_temp[cols_to_keep]

def opcion4(shp_path, output_dir="output", layer=None, indices=["NDVI","AR","AS1"], start_date="2017-01-01", end_date="2024-12-31"):
    """
    Función todo-en-uno que descarga series de PlanetaryComputer, calcula marcadores
    y guarda un CSV final listo para opcion2.
    """
    os.makedirs(output_dir, exist_ok=True)

    # 1️⃣ Cargar shapefile/GeoPackage
    if layer:
        gdf = gpd.read_file(shp_path, layer=layer)
    else:
        gdf = gpd.read_file(shp_path)

    if "provincia" not in gdf.columns:
        raise ValueError("El shapefile debe contener la columna 'provincia'.")

    # 2️⃣ Iterar por provincias
    provincias = gdf["provincia"].unique()
    for prov in tqdm(provincias, desc="Procesando provincias"):
        gdf_prov = gdf[gdf["provincia"] == prov].copy()

        # 3️⃣ Descargar series temporales usando process_gdf
        df_series = process_gdf(
            gdf_prov,
            indices=indices,
            start=start_date,
            end=end_date,
            crs_data_epsg=32630,
            resolution=20,
            fecha_corte="2022-01-24",
            params=None,
            progress=False,
        )

        # 4️⃣ Generar ID_STATS
        #id_cols = ['provincia','municipio','agregado','zona','poligono','parcela','recinto']
        #df_series['ID_STATS'] = df_series.apply(lambda x: '-'.join(str(x[c]) for c in id_cols), axis=1)

        # 5️⃣ Pivotar de largo a ancho: columnas por fecha+índice
        df_wide = df_series.pivot_table(
            index="ID_STATS",
            columns="fecha",
            values=indices,
            aggfunc='first'
        )

        # Flatten MultiIndex columnas
        df_wide.columns = [
            f"{pd.to_datetime(col[1]).strftime('%Y%m%d')}T_{col[0]}" for col in df_wide.columns
        ]
        df_wide.reset_index(inplace=True)

        # 6️⃣ Crear columnas resumen NDVI, AR, AS1 para calcular_marcadores
        for idx in indices:
            matching_cols = [c for c in df_wide.columns if c.endswith(f"T_{idx}")]
            if matching_cols:
                df_wide[idx] = df_wide[matching_cols].apply(lambda row: row.values, axis=1)

        # 7️⃣ Calcular marcadores
        df_markers = preparar_df_para_marcadores(df_wide, indices=indices)
        df_markers = upm.calcular_marcadores(df_markers)
        if df_markers is None or df_markers.empty:
            print(f"⚠️ df_markers vacío para provincia {prov}. Se omite.")
            continue

        # 8️⃣ Seleccionar columnas finales
        final_columns = ['ID_STATS','TAM','INTERVALS','ANUALCYCLE','TAM_FLAG','STABILITY_STATUS','NDVI','AR','AS1']
        missing_cols = [c for c in final_columns if c not in df_markers.columns]
        for c in missing_cols:
            if c in df_wide.columns:
                df_markers[c] = df_wide.loc[df_markers.index, c]
                df_markers[c] = df_markers[c].apply(lambda x: ','.join(map(str, x)) if hasattr(x, '__iter__') else x)
            else:
                # Si no existe, rellenar con ceros
                df_markers[c] = [0]*len(df_markers)
        df_markers = df_markers[final_columns]

        # 9️⃣ Guardar CSV por provincia
        df_markers = df_markers.sort_values('ID_STATS')
        out_csv = os.path.join(output_dir, f'marcadores_{prov}.csv')
        df_markers.to_csv(out_csv, index=False, sep=';')
        print(f"Provincia {prov} procesada. CSV guardado en: {out_csv}")


    print("✅ Todas las provincias procesadas.")
    return True
    

def opcion6(ar_path, as1_path, ndvi_path, dates_path, set_parameters=False):
    """Calcula los marcadores de un conjunto de datos de parcelas con los archivos individuales de AR, AS1 y NDVI.

    Args:
        ar_path (str): Ruta al archivo CSV de AR.
        as1_path (str): Ruta al archivo CSV de AS1.
        ndvi_path (str): Ruta al archivo CSV de NDVI.
    """
    ar_df = pd.read_csv(ar_path)
    if ar_df.shape[1] < 2:
        ar_df = pd.read_csv(ar_path, sep=';')
    as1_df = pd.read_csv(as1_path)
    if as1_df.shape[1] < 2:
        as1_df = pd.read_csv(as1_path, sep=';')
    ndvi_df = pd.read_csv(ndvi_path)
    if ndvi_df.shape[1] < 2:
        ndvi_df = pd.read_csv(ndvi_path, sep=';')
    fechas_df = pd.read_csv(dates_path, header=0, names=['fecha'])
    
    if ndvi_df.shape[0] != ar_df.shape[0] or ndvi_df.shape[0] != as1_df.shape[0]:
        raise ValueError("Los archivos AR, AS1 y NDVI deben tener el mismo número de filas.")
    # asearse de que las columnas de fecha son del tipo datetime
    fechas_df['fecha'] = pd.to_datetime(fechas_df['fecha'], format='%d/%m/%Y', errors='coerce')
    
    # obtener la columna que conteniene *ID*
    id_col = [col for col in ar_df.columns if 'ID' in col][0]
    print(f'Columna de ID encontrada: {id_col}')
    # renombrar las columnas con el sufijo correspondiente y la fecha correspondiente
    for i, row in fechas_df.iterrows():
        fecha = row['fecha'].strftime('%Y%m%d')
        ar_df.rename(columns={ar_df.columns[i+1]: f"{fecha}T_AR"}, inplace=True)
        as1_df.rename(columns={as1_df.columns[i+1]: f"{fecha}T_AS1"}, inplace=True)
        ndvi_df.rename(columns={ndvi_df.columns[i+1]: f"{fecha}T_NDVI"}, inplace=True)
    # unir los dataframes por la columna ID
    df = ar_df.merge(as1_df, on=id_col, how="inner", validate="many_to_many").merge(ndvi_df, on=id_col, how="inner", validate="many_to_many")
    output_path = os.path.dirname(ar_path)
    df.to_csv(os.path.join(output_path, 'series.csv'), index=False)
    if set_parameters:
        upm.parametrizar(df.copy())
        
    df = upm.calcular_marcadores(df)
    df.to_csv(os.path.join(output_path, f'marcadores-{upm.umbral_tam}-{upm.umbral_days}-{upm.umbral_productivity}.csv'), index=False, sep=';')
    print('Marcadores calculados y guardados en:', os.path.join(output_path,  f'marcadores-{upm.umbral_tam}-{upm.umbral_days}-{upm.umbral_productivity}.csv'))
    