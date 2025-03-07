# -*- coding: utf-8 -*-

import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from customtkinter import filedialog
from tkcalendar import *
from ttkwidgets.autocomplete import AutocompleteCombobox
from PIL import Image
import subprocess
import sys
import os
from threading import Thread
from datetime import datetime

import geopandas as gpd
from tqdm import tqdm
from datetime import date
from sqlalchemy import create_engine
import shapely
from shapely.geometry import MultiPolygon, box, LineString
from shapely.wkt import loads, dumps
import numpy as np
import pandas as pd
import warnings
from scipy.spatial import ConvexHull

# ------------------------------------------------------------------------------
# CONFIGURACIONES GLOBALES
# ------------------------------------------------------------------------------
ctk.set_default_color_theme("green")
ctk.set_appearance_mode('light')

warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)
warnings.filterwarnings("ignore", message="`unary_union` returned None due to all-None GeoSeries")

# DICCIONARIOS Y VARIABLES GLOBALES
provincias_españa = [
    "Álava", "Albacete", "Alicante", "Almería", "Asturias", "Ávila", 
    "Badajoz", "Baleares", "Barcelona", "Burgos", 
    "Cáceres", "Cádiz", "Cantabria", "Castellón", "Ceuta", "Ciudad Real", 
    "Córdoba", "Cuenca", 
    "Girona", "Granada", "Guadalajara", "Guipúzcoa", 
    "Huelva", "Huesca", 
    "Jaén", 
    "La Coruña", "La Rioja", "Las Palmas", "León", "Lleida", "Lugo", 
    "Madrid", "Málaga", "Melilla", "Murcia", 
    "Navarra", 
    "Orense", 
    "Palencia", "Pontevedra", 
    "Salamanca", "Santa Cruz de Tenerife", "Segovia", "Sevilla", "Soria", 
    "Tarragona", "Teruel", "Toledo", 
    "Valencia", "Valladolid", "Vizcaya", 
    "Zamora", "Zaragoza"
]

provincias_sencillas = {
    'Álava': 'Alava',
    'Albacete': 'Albacete',
    'Alicante': 'Alicante',
    'Almería': 'Almeria',
    'Asturias': 'Asturias',
    'Ávila': 'Avila',
    'Badajoz': 'Badajoz',
    'Baleares': 'Baleares',
    'Barcelona': 'Barcelona',
    'Burgos': 'Burgos',
    'Cáceres': 'Caceres',
    'Cádiz': 'Cadiz',
    'Cantabria': 'Cantabria',
    'Castellón': 'Castellon',
    'Ceuta': 'Ceuta',
    'Ciudad Real': 'CiudadReal',
    'Córdoba': 'Cordoba',
    'Cuenca': 'Cuenca',
    'Girona': 'Girona',
    'Granada': 'Granada',
    'Guadalajara': 'Guadalajara',
    'Guipúzcoa': 'Guipuzcoa',
    'Huelva': 'Huelva',
    'Huesca': 'Huesca',
    'Jaén': 'Jaen',
    'La Coruña': 'Coruña',
    'León': 'Leon',
    'Lleida': 'Lleida',
    'Lugo': 'Lugo',
    'La Rioja': 'LaRioja',
    'Madrid': 'Madrid',
    'Málaga': 'Malaga',
    'Melilla': 'Melilla',
    'Murcia': 'Murcia',
    'Navarra': 'Navarra',
    'Ourense': 'Ourense',
    'Palencia': 'Palencia',
    'Las Palmas': 'LasPalmas',
    'Pontevedra': 'Pontevedra',
    'Salamanca': 'Salamanca',
    'Santa Cruz de Tenerife': 'Tenerife',
    'Segovia': 'Segovia',
    'Sevilla': 'Sevilla',
    'Soria': 'Soria',
    'Tarragona': 'Tarragona',
    'Teruel': 'Teruel',
    'Toledo': 'Toledo',
    'Valencia': 'Valencia',
    'Valladolid': 'Valladolid',
    'Vizcaya': 'Vizcaya',
    'Zamora': 'Zamora',
    'Zaragoza': 'Zaragoza'
}

# Diccionario de usos del suelo (para checkboxes en la GUI)
usos_suelo = {
    "AG": "CORRIENTES Y SUPERFICIES DE AGUA",
    "CA": "VIALES",
    "CI": "CITRICOS",
    "CO": "CONTORNO OLIVAR",
    "ED": "EDIFICACIONES",
    "FO": "FORESTAL",
    "FY": "FRUTALES",
    "IM": "IMPRODUCTIVOS",
    "IV": "INVERNADEROS Y CULTIVOS BAJO PLASTICO",
    "OF": "OLIVAR - FRUTAL",
    "OV": "OLIVAR",
    "PA": "PASTO CON ARBOLADO",
    "PR": "PASTO ARBUSTIVO",
    "PS": "PASTIZAL",
    "TA": "TIERRAS ARABLES",
    "TH": "HUERTA",
    "VF": "VIÑEDO - FRUTAL",
    "VI": "VIÑEDO",
    "VO": "VIÑEDO - OLIVAR",
    "ZC": "ZONA CONCENTRADA NO INCLUIDA EN LA ORTOF",
    "ZU": "ZONA URBANA",
    "ZV": "ZONA CENSURADA",
    "FS": "FRUTOS SECOS",
    "FL": "FRUTOS SECOS Y OLIVAR",
    "FV": "FRUTOS SECOS Y VIÑEDO",
    "IS": "ISLAS",
    "OC": "Asociación Olivar-Cítricos",
    "CV": "Asociación Cítricos-Viñedo",
    "CF": "Asociación Cítricos-Frutales",
    "CS": "Asociación Cítricos-Frutales de cáscara",
    "FF": "Asociación Frutales-Frutales de cáscara",
    "EP": "ELEMENTO DEL PAISAJE",
    "MT": "MATORRAL",
    "OP": "Otros cultivos Permanentes"
}

# Variables globales para configuración
user_sql_url = None
ogr_path = None

# ------------------------------------------------------------------------------
# FEGA_REC_APP: Lógica principal de procesamiento
# ------------------------------------------------------------------------------
class FEGA_REC_APP:
    message = ''
    percentaje = 0
    outpath = ''

    @staticmethod
    def config_csv(csv_path: str) -> dict:
        df = pd.read_csv(csv_path, delimiter=';')
        years = [str(i) for i in range(2020, 2024 + 1)]
        relevant_columns = ['Comunidad_autonoma', 'Nombre_base_datos'] + years
        df = df[relevant_columns]
        config = {}
        for _, row in df.iterrows():
            comunidad = row['Comunidad_autonoma']
            database_name = row['Nombre_base_datos']
            dates = {year: row[year] for year in years}
            config[comunidad] = (database_name, dates)
        return config
    
    @staticmethod
    def create_prov_dict(config):
        return {
            'Alava':        (1,  config['PAIS-VASCO']),
            'Albacete':     (2,  config['CASTILLA-LA-MANCHA']),
            'Alicante':     (3,  config['VALENCIANA']),
            'Almeria':      (4,  config['ANDALUCÍA']),
            'Avila':        (5,  config['CASTILLA-Y-LEÓN']),
            'Badajoz':      (6,  config['EXTREMADURA']),
            'Baleares':     (7,  config['BALEARES']),
            'Barcelona':    (8,  config['CATALUÑA']),
            'Burgos':       (9,  config['CASTILLA-Y-LEÓN']),
            'Caceres':      (10, config['EXTREMADURA']),
            'Cadiz':        (11, config['ANDALUCÍA']),
            'Castellon':    (12, config['VALENCIANA']),
            'CiudadReal':   (13, config['CASTILLA-LA-MANCHA']),
            'Cordoba':      (14, config['ANDALUCÍA']),
            'Coruña':       (15, config['GALICIA']),
            'Cuenca':       (16, config['CASTILLA-LA-MANCHA']),
            'Girona':       (17, config['CATALUÑA']),
            'Granada':      (18, config['ANDALUCÍA']),
            'Guadalajara':  (19, config['CASTILLA-LA-MANCHA']),
            'Guipuzcoa':    (20, config['PAIS-VASCO']),
            'Huelva':       (21, config['ANDALUCÍA']),
            'Huesca':       (22, config['ARAGÓN']),
            'Jaen':         (23, config['ANDALUCÍA']),
            'Leon':         (24, config['CASTILLA-Y-LEÓN']),
            'Lleida':       (25, config['CATALUÑA']),
            'LaRioja':      (26, config['LA-RIOJA']),
            'Lugo':         (27, config['GALICIA']),
            'Madrid':       (28, config['MADRID']),
            'Malaga':       (29, config['ANDALUCÍA']),
            'Murcia':       (30, config['MURCIA']),
            'Navarra':      (31, config['NAVARRA']),
            'Ourense':      (32, config['GALICIA']),
            'Asturias':     (33, config['ASTURIAS']),
            'Palencia':     (34, config['CASTILLA-Y-LEÓN']),
            'LasPalmas':    (35, config['CANARIAS']),
            'Pontevedra':   (36, config['GALICIA']),
            'Salamanca':    (37, config['CASTILLA-Y-LEÓN']),
            'Tenerife':     (38, config['CANARIAS']),
            'Cantabria':    (39, config['CANTABRIA']),
            'Segovia':      (40, config['CASTILLA-Y-LEÓN']),
            'Sevilla':      (41, config['ANDALUCÍA']),
            'Soria':        (42, config['CASTILLA-Y-LEÓN']),
            'Tarragona':    (43, config['CATALUÑA']),
            'Teruel':       (44, config['ARAGÓN']),
            'Toledo':       (45, config['CASTILLA-LA-MANCHA']),
            'Valencia':     (46, config['VALENCIANA']),
            'Valladolid':   (47, config['CASTILLA-Y-LEÓN']),
            'Vizcaya':      (48, config['PAIS-VASCO']),
            'Zamora':       (49, config['CASTILLA-Y-LEÓN']),
            'Zaragoza':     (50, config['ARAGÓN']),
        }
    
    @staticmethod
    def corregir_geometrias(gdf):
        gdf = gdf.copy(deep=True)
        gdf = gdf[gdf.geometry.notna()]
        gdf = gdf[~gdf.geometry.is_empty]
        
        def _corregir(geom):
            if not geom.is_valid:
                geom = shapely.make_valid(geom)
            geom = shapely.force_2d(geom)
            geom = shapely.simplify(geom, tolerance=0.1)
            geom = shapely.set_precision(geom, grid_size=0.1)
            if geom.geom_type in ['MultiPolygon', 'MultiLineString']:
                geom = shapely.ops.unary_union(geom)
                if geom.geom_type == 'GeometryCollection':
                    geom = shapely.geometry.MultiPolygon(
                        [g for g in geom.geoms if isinstance(g, (shapely.geometry.Polygon, shapely.geometry.MultiPolygon))]
                    )
            geom = shapely.set_precision(geom, grid_size=2)
            return geom
        
        gdf.geometry = gdf.geometry.apply(_corregir)
        return gdf
    
    @staticmethod
    def filiformes(input_gdf):
        gdf = input_gdf.copy()
        gdf['perimeter_area_ratio'] = gdf.geometry.length / gdf.geometry.area
        mask = gdf['perimeter_area_ratio'] <= 4
        gdf = gdf.drop(columns=['perimeter_area_ratio'])
        return gdf[mask].reset_index(drop=True)
    
    @staticmethod
    def eliminate_overlaps_geodataframe(gdf):
        gdf = gdf.copy()
        if not isinstance(gdf, gpd.GeoDataFrame):
            raise TypeError("Input must be a GeoDataFrame")
        gdf['area'] = gdf.geometry.area
        to_delete = []
        sindex = gdf.sindex
        
        for idx1, geom1 in enumerate(gdf.geometry):
            area1 = gdf.iloc[idx1].area
            possible_matches_idx = list(sindex.intersection(geom1.bounds))
            for idx2 in possible_matches_idx:
                if idx2 != idx1:
                    geom2 = gdf.geometry.iloc[idx2]
                    area2 = gdf.iloc[idx2].area
                    if geom1.intersects(geom2):
                        try:
                            intersection_area = geom1.intersection(geom2).area
                            if intersection_area > min(area1, area2) * 0.01:
                                if area1 > area2:
                                    to_delete.append(idx2)
                                else:
                                    to_delete.append(idx1)
                                    break
                        except Exception:
                            continue
        gdf = gdf.drop(index=to_delete).reset_index(drop=True)
        return gdf
    
    @staticmethod
    def clip_dfs(list_dfs, clip_geom):
        for i in range(len(list_dfs)):
            list_dfs[i] = gpd.clip(list_dfs[i], clip_geom)
        return list_dfs
    
    @staticmethod
    def main(start, end, out_dir, provi, user_url, clip_path=None, ogr2ogr_path=None, usos_sel=None):
        from tkinter import messagebox
        
        if usos_sel is None:
            usos_sel = ['PS', 'PA', 'PR', 'FY', 'OV', 'VI']
        
        FEGA_REC_APP.message = ''
        FEGA_REC_APP.percentaje = 0
        FEGA_REC_APP.outpath = ''
        
        ini = start
        fin = end
        provincia = provi
        outdir = out_dir
        
        FEGA_REC_APP.message = "Cargando configuración..."
        config = FEGA_REC_APP.config_csv(r'./config/CSV_CONFIG.csv')
        
        prov_dict = FEGA_REC_APP.create_prov_dict(config)
        
        if provincia not in prov_dict:
            messagebox.showerror("Error", f"La provincia {provincia} no está en el diccionario.")
            return
        num_prov, (db_name, dates_dict) = prov_dict[provincia]
        
        # Conexión a la BBDD
        url_db = f"{user_url}/{db_name}"
        sql_engine = create_engine(url_db)
        
        FEGA_REC_APP.message = 'Extrayendo recintos...'
        recintos_df = []
        dif = fin + 1 - ini
        it = 12.5 / dif if dif != 0 else 12.5
        
        for year in tqdm(range(ini, fin + 1)):
            year_s = str(year)
            next_s = str(year + 1)
            fecha_inicio = date.fromisoformat(dates_dict[year_s])
            fecha_fin = date.fromisoformat(dates_dict[next_s]) if next_s in dates_dict else date.fromisoformat(dates_dict[year_s])
            
            query = f"""
SELECT DISTINCT(dn_oid),
       ST_MakeValid(ST_TRANSFORM(dn_geom,32630),'method=structure keepcollapsed=false') as dn_geom,
       dn_initialdate,
       dn_enddate,
       cap_prevalente as cp_{year},
       uso_sigpac as uso_sigpac_{year},
       incidencias as incidencias_{year},
       concat(provincia,'-',municipio,'-', agregado,'-',zona,'-',poligono,'-',parcela,'-',recinto) as id_recinto_{year},
       ST_AREA(ST_TRANSFORM(dn_geom,32630)) as area_m2
FROM t$recinto_ex
WHERE (dn_initialdate <= '{fecha_fin}' AND
      ((dn_enddate BETWEEN '{fecha_inicio}' AND '{fecha_fin}') or dn_enddate is null))
  AND (dn_initialdate_1 <= '{fecha_fin}' AND
      ((dn_enddate_1 BETWEEN '{fecha_inicio}' AND '{fecha_fin}') or dn_enddate_1 is null))
  AND provincia = {num_prov}
  AND uso_sigpac <> 'CA' AND uso_sigpac <> 'AG' AND uso_sigpac <> 'ZU' 
  AND uso_sigpac <> 'ED' AND uso_sigpac <> 'ZC' AND uso_sigpac <> 'ZV' AND uso_sigpac <> 'IV'
  AND ST_AREA(ST_TRANSFORM(dn_geom,32630)) > 5000;
"""
            try:
                df_tmp = gpd.GeoDataFrame.from_postgis(query, sql_engine, geom_col='dn_geom')
                df_tmp = df_tmp[df_tmp['dn_geom'].notna()]
                df_tmp['dn_initialdate'] = df_tmp['dn_initialdate'].astype(str)
                df_tmp['dn_enddate'] = df_tmp['dn_enddate'].astype(str)
                recintos_df.append(df_tmp)
            except Exception as e:
                tqdm.write(f"Error al extraer recintos de {year}: {e}")
            FEGA_REC_APP.percentaje += it
        
        FEGA_REC_APP.message = 'Extrayendo líneas de declaración...'
        lineas_df = []
        it_lineas = 12.5 / dif if dif != 0 else 12.5
        
        for year in tqdm(range(ini, fin + 1)):
            year_s = str(year)
            next_s = str(year + 1)
            fecha_inicio = date.fromisoformat(dates_dict[year_s])
            fecha_fin = date.fromisoformat(dates_dict[next_s]) if next_s in dates_dict else date.fromisoformat(dates_dict[year_s])
            
            query = f"""
SELECT ld.dn_pk as ld_pk_{year},
       ld.parc_producto as parc_producto_{year},
       ld.dn_oid,
       ld.dn_surface,
       ld.dn_initialdate,
       ld.dn_enddate,
       ST_MakeValid(ST_TRANSFORM(ld.dn_geom,32630),'method=structure keepcollapsed=false') as dn_geom,
       ST_AREA(ST_TRANSFORM(ld.dn_geom,32630)) as area_m2,
       concat(provincia,'-',municipio,'-', agregado,'-',zona,'-',poligono,'-',parcela,'-',recinto) as id_recinto_{year}
FROM t$linea_declaracion AS ld
WHERE ((ld.dn_initialdate <= '{fecha_fin}' AND 
       (ld.dn_enddate BETWEEN '{fecha_inicio}' AND '{fecha_fin}') OR ld.dn_enddate IS NULL) 
   AND ld.provincia = {num_prov})
  AND (ST_AREA(ST_TRANSFORM(ld.dn_geom,32630)) > 5000);
"""
            try:
                df_tmp = gpd.GeoDataFrame.from_postgis(query, sql_engine, geom_col='dn_geom')
                df_tmp = df_tmp[df_tmp['dn_geom'].notna()]
                df_tmp['dn_initialdate'] = df_tmp['dn_initialdate'].astype(str)
                df_tmp['dn_enddate'] = df_tmp['dn_enddate'].astype(str)
                lineas_df.append(df_tmp)
            except Exception as e:
                tqdm.write(f"Error al extraer lineas de {year}: {e}")
            FEGA_REC_APP.percentaje += it_lineas
        
        # 6) Si hay un shapefile de clip, recortamos
        if clip_path:
            FEGA_REC_APP.message = "Clipping con ROI..."
            cliped = gpd.read_file(clip_path)
            cliped = cliped.to_crs(epsg=32630)
            recintos_df = FEGA_REC_APP.clip_dfs(recintos_df, cliped)
            lineas_df = FEGA_REC_APP.clip_dfs(lineas_df, cliped)
        
        FEGA_REC_APP.message = "Creando recintos declarados..."
        rd_dir = os.path.join(outdir, f"{num_prov}.gpkg")
        layers_out = []
        cur_year = ini
        it_rd = 12.5 / len(recintos_df) if recintos_df else 1
        
        for i in tqdm(range(len(recintos_df))):
            rc = gpd.GeoDataFrame(recintos_df[i], geometry='dn_geom', crs=32630)
            rc = FEGA_REC_APP.corregir_geometrias(rc)
            rc = FEGA_REC_APP.filiformes(rc)
            rc = FEGA_REC_APP.eliminate_overlaps_geodataframe(rc)
            
            ld = gpd.GeoDataFrame(lineas_df[i], geometry='dn_geom', crs=32630)
            ld = FEGA_REC_APP.corregir_geometrias(ld)
            ld = FEGA_REC_APP.filiformes(ld)
            ld = FEGA_REC_APP.eliminate_overlaps_geodataframe(ld)
            
            over_df = gpd.overlay(rc, ld, how='union', make_valid=True, keep_geom_type=True)
            over_df = FEGA_REC_APP.filiformes(over_df)
            over_df = FEGA_REC_APP.corregir_geometrias(over_df)
            over_df = over_df.drop_duplicates(subset='geometry')
            
            campos_export = [
                f'uso_sigpac_{cur_year}',
                f'parc_producto_{cur_year}',
                f'incidencias_{cur_year}',
                f'cp_{cur_year}',
                f'id_recinto_{cur_year}',
                'geometry'
            ]
            over_df = over_df[campos_export]
            over_df[f'incidencias_{cur_year}'] = over_df[f'incidencias_{cur_year}'].astype(str)
            over_df = gpd.GeoDataFrame(over_df, geometry='geometry', crs=rc.crs)
            
            # clasificacion en 'est_aband_{cur_year}'
            over_df[f'est_aband_{cur_year}'] = over_df.apply(
                lambda row: ('SLD' if pd.isna(row[f'parc_producto_{cur_year}']) or row[f'parc_producto_{cur_year}'] == 0 else
                             'A'   if '117' in row[f'incidencias_{cur_year}'] or '199' in row[f'incidencias_{cur_year}'] else
                             'M5'  if '177' in row[f'incidencias_{cur_year}'] or '186' in row[f'incidencias_{cur_year}'] else
                             'NA'  if row[f'uso_sigpac_{cur_year}'] == 'OV' and row[f'parc_producto_{cur_year}'] == 101 else
                             'PA'  if row[f'uso_sigpac_{cur_year}'] == 'OV' and row[f'parc_producto_{cur_year}'] != 101 else
                             'NA_150' if row[f'uso_sigpac_{cur_year}'] == 'OV' and row[f'parc_producto_{cur_year}'] == 150 else
                             'SRC' if pd.isna(row[f'uso_sigpac_{cur_year}']) else
                             'NA'  if (
                                 row[f'uso_sigpac_{cur_year}'] == 'FY' and
                                 (
                                     row[f'parc_producto_{cur_year}'] in [104,105,106,107,108,109,110,111,112,113] or
                                     row[f'parc_producto_{cur_year}'] in [200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218] or
                                     row[f'parc_producto_{cur_year}'] in list(range(300,350))
                                 )
                             ) else
                             'NA_150' if row[f'uso_sigpac_{cur_year}'] == 'FY' and row[f'parc_producto_{cur_year}'] == 150 else
                             'NA' if row[f'uso_sigpac_{cur_year}'] in ['PR','PS','PA'] and row[f'parc_producto_{cur_year}'] in [61,62,63,64,65] else
                             'PA' if row[f'uso_sigpac_{cur_year}'] in ['PR','PS','PA'] and not (61 <= row[f'parc_producto_{cur_year}'] <= 65) else
                             'NA_150' if row[f'uso_sigpac_{cur_year}'] in ['PR','PS','PA'] and row[f'parc_producto_{cur_year}'] == 150 else
                             'Uso_alt'),
                axis=1
            )
            
            over_df['geometry'] = over_df['geometry'].apply(
                lambda x: MultiPolygon([x]) if x.geom_type == 'Polygon' else x
            )
            
            layer_name = f'rd_{num_prov}_{cur_year}'
            over_df.to_file(rd_dir, driver='GPKG', layer=layer_name)
            layers_out.append(layer_name)
            cur_year += 1
            FEGA_REC_APP.percentaje += it_rd
        
        FEGA_REC_APP.message = "Creando capa CRONO..."
        def overlay_crono(layers, gpkg_path):
            if len(layers) < 2:
                return layers[0] if layers else None
            overlay_type = "union"
            input_layer = layers[0]
            for i in range(1, len(layers)):
                output_layer = f"{input_layer}_o_{layers[i]}"
                try:
                    in_df = gpd.read_file(gpkg_path, layer=input_layer)
                    in_df = FEGA_REC_APP.filiformes(in_df)
                    in_df = FEGA_REC_APP.corregir_geometrias(in_df)
                    in_df = in_df[in_df.geom_type.isin(['Polygon','MultiPolygon'])]
                    
                    cur_df = gpd.read_file(gpkg_path, layer=layers[i])
                    cur_df = FEGA_REC_APP.filiformes(cur_df)
                    cur_df = FEGA_REC_APP.corregir_geometrias(cur_df)
                    cur_df = cur_df[cur_df.geom_type.isin(['Polygon','MultiPolygon'])]
                    
                    over_df = gpd.overlay(in_df, cur_df, how=overlay_type, make_valid=True, keep_geom_type=True)
                    over_df = FEGA_REC_APP.corregir_geometrias(over_df)
                    over_df = FEGA_REC_APP.filiformes(over_df)
                    over_df = over_df[over_df.geom_type.isin(['Polygon','MultiPolygon'])]
                    over_df.to_file(gpkg_path, driver='GPKG', layer=output_layer)
                except Exception as e:
                    print(f"Error en overlay_crono: {e}")
                input_layer = output_layer
            return input_layer
        
        final_layer = overlay_crono(layers_out, rd_dir)
        
        FEGA_REC_APP.message = "Exportando cronología final..."
        if final_layer:
            df_crono = gpd.read_file(rd_dir, layer=final_layer)
            
            def calcular_acrono(row, years):
                res = ''
                for y in years:
                    aband = row.get(f'est_aband_{y}', 'None')
                    if aband == 'A':
                        res += '0'
                    elif aband == 'NA':
                        res += '1'
                    elif aband == 'SLD':
                        res += '2'
                    elif aband == 'SRC':
                        res += '3'
                    elif aband == 'PA':
                        res += '4'
                    elif aband == 'None':
                        res += '9'
                    else:
                        res += '5'
                return res
            
            def calcular_pcrono(row, years):
                res = []
                for y in years:
                    val = row.get(f'parc_producto_{y}', np.nan)
                    if pd.isna(val):
                        val = 0
                    res.append(str(int(val)))
                return '_'.join(res)
            
            def calcular_ucrono(row, years):
                res = []
                for y in years:
                    uso = row.get(f'uso_sigpac_{y}', 'NU')
                    res.append(uso if pd.notna(uso) else 'NU')
                return '_'.join(res)
            
            def calcular_icrono(row, years):
                res = []
                for y in years:
                    inc = row.get(f'incidencias_{y}', 'NA')
                    res.append(inc if pd.notna(inc) else 'NA')
                return '_'.join(res)
            
            years_list = list(range(ini, fin + 1))
            df_crono['a_crono'] = df_crono.apply(lambda r: calcular_acrono(r, years_list), axis=1)
            df_crono['p_crono'] = df_crono.apply(lambda r: calcular_pcrono(r, years_list), axis=1)
            df_crono['u_crono'] = df_crono.apply(lambda r: calcular_ucrono(r, years_list), axis=1)
            df_crono['i_crono'] = df_crono.apply(lambda r: calcular_icrono(r, years_list), axis=1)
            df_crono['id_recinto'] = df_crono.apply(lambda r: r.get(f'id_recinto_{fin}', ''), axis=1)
            
            # Filtrar por usos_sel si fuera necesario:
            # pattern = '|'.join(usos_sel)
            # df_crono = df_crono[df_crono['u_crono'].str.contains(pattern)]
            
            final_export_layer = f"CRONO_FIN_{num_prov}"
            campos_export = ['geometry','a_crono','p_crono','u_crono','i_crono','id_recinto']
            df_crono[campos_export].to_file(rd_dir, driver='GPKG', layer=final_export_layer)
        
        FEGA_REC_APP.percentaje = 100
        FEGA_REC_APP.message = "Proceso finalizado"
        FEGA_REC_APP.outpath = rd_dir


# ------------------------------------------------------------------------------
# CLASE PRINCIPAL: Fega
# ------------------------------------------------------------------------------
class Fega(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("FEGAPP")
        self.geometry("750x550")
        self.iconbitmap("./img/IconoFegaApp.ico")
        self.resizable(0,0)
        
        self.grid_columnconfigure((0,1), weight=1)
        self.grid_rowconfigure([i for i in range(10)], weight=1)
        
        self.menu = Menu(self)
        self.menu.grid(row=1, column=0, rowspan=9, sticky='nwes', padx=5, pady=5, columnspan=2)
        
        self.createWidgets()
        
    def createWidgets(self):
        img_logo = ctk.CTkImage(light_image=Image.open("./img/composicion.png"), size=(300,100))
        self.logo = ctk.CTkLabel(self, image=img_logo, text="")
        self.logo.grid(row=0, column=0, padx=10, sticky='wns')
        
        self.btn_config = ctk.CTkButton(
            master=self,
            width=80,
            text="Image Download",
            font=("Helvetica", 20),
            fg_color="Grey",
            command=self.open_sentinel_app
        )
        self.btn_config.grid(row=0, column=1, sticky="w", padx=40)
    
    def setConfig(self):
        global user_sql_url
        conf = open('./config/config.txt', 'w')
        user_sql_url = simpledialog.askstring('SQL user URL', 'Write the database url for the features extraction')
        conf.write(user_sql_url)
    
    def open_sentinel_app(self):
        top = ctk.CTkToplevel(self)
        top.title("Sentinel-2 Index Processor")
        top.geometry("540x350")
        SentinelIndexProcessorApp(top).pack(expand=True, fill="both")
        top.wm_transient(self)


# ------------------------------------------------------------------------------
# CLASE PARA DESCARGA Y PROCESADO SENTINEL
# ------------------------------------------------------------------------------
class SentinelIndexProcessorApp(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        
        self.start_date_var = tk.StringVar()
        self.end_date_var = tk.StringVar()
        self.index_type_var = tk.StringVar(value="NDVI")
        self.resolution_var = tk.IntVar(value=10)
        self.output_dir_var = tk.StringVar()
        self.tile_var = tk.StringVar()
        self.clip_path_var = tk.StringVar()
        self.driver_var = tk.StringVar(value="ENVI")
        
        self.index_options = [
            'NDVI', 'GNDVI', 'LAI', 'EVI', 'LAI_EVI', 'SR', 'NIR', 'RED',
            'SAVI', 'fAPAR', 'AR', 'AS1', 'SASI', 'ANIR', 'ARE1', 'ARE2',
            'ARE3', 'NBR','CONC', 'B1', 'B2', 'B3', 'B4', 'B5', 'B6',
            'B7', 'B8', 'B9', 'B10', 'B11', 'B12', 'SCL'
        ]
        self.create_widgets()

    def create_widgets(self):
        ctk.CTkLabel(self, text="Start Date:").grid(row=0, column=0, pady=(25,5), padx=(30,10), sticky="w")
        ctk.CTkEntry(self, textvariable=self.start_date_var, width=200).grid(row=0, column=1, pady=(25,5), padx=(0,5))
        ctk.CTkButton(self, text="📅", command=lambda: self.open_calendar('start')).grid(row=0, column=2, padx=(5,5), pady=(25,5))

        ctk.CTkLabel(self, text="End Date:").grid(row=1, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkEntry(self, textvariable=self.end_date_var, width=200).grid(row=1, column=1, pady=5, padx=(0,5))
        ctk.CTkButton(self, text="📅", command=lambda: self.open_calendar('end')).grid(row=1, column=2, padx=5)

        ctk.CTkLabel(self, text="Index Type:").grid(row=2, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkComboBox(self, values=self.index_options, variable=self.index_type_var).grid(row=2, column=1, pady=5, padx=(0,5), sticky="we")

        ctk.CTkLabel(self, text="Resolution:").grid(row=3, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkComboBox(self, values=["10", "20", "60"], variable=self.resolution_var).grid(row=3, column=1, pady=5, padx=(0,5), sticky="we")

        ctk.CTkLabel(self, text="Output Directory:").grid(row=4, column=0, pady=5, padx=(30,10), sticky="w")
        ctk.CTkEntry(self, textvariable=self.output_dir_var, width=200).grid(row=4, column=1, pady=5, padx=(0,5))
        ctk.CTkButton(self, text="📁", command=self.select_output_directory).grid(row=4, column=2, padx=5)

        ctk.CTkLabel(self, text="ROI:").grid(row=5, column=0, pady=5, padx=(30,10), sticky="w")
        self.roi = ctk.CTkEntry(self, width=100, placeholder_text='Write the Tile name or shapefile')
        self.roi.grid(row=5, column=1, pady=5, padx=(0,5), sticky="we")
        self.check = ctk.CTkCheckBox(self, text="Shapefile", command=self.select_clip_path)
        self.check.grid(row=5, column=2, padx=5)

        ctk.CTkLabel(self, text="Output Format:").grid(row=7, column=0, pady=5, sticky="w", padx=(30,10))
        ctk.CTkComboBox(self, values=["ENVI", "NetCDF"], variable=self.driver_var).grid(row=7, column=1, pady=5, padx=(0,5), sticky="we")

        ctk.CTkButton(self, text="Process Sentinel-2 Data", fg_color="Blue", command=self.process_data).grid(row=8, column=0, columnspan=3, pady=20)

    def process_data(self):
        if not all([self.start_date_var.get(), self.end_date_var.get(), self.output_dir_var.get(), self.roi.get()]):
            messagebox.showerror("Error", "Please fill all required fields")
            return
        try:
            cmd = [
                sys.executable,
                r"lib/descarga_planet.py",
                self.output_dir_var.get(),
                self.start_date_var.get(),
                self.end_date_var.get(),
                self.index_type_var.get(),
                str(self.resolution_var.get()),
                self.driver_var.get()
            ]
            if not self.check.get():
                cmd.extend(["--tile", self.roi.get()])
            else:
                cmd.extend(["--clip_path", self.roi.get()])

            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            output_window = tk.Toplevel(self.master)
            output_window.title("Processing Output")
            output_window.geometry("600x400")
            
            text_area = tk.Text(output_window, wrap=tk.WORD)
            text_area.pack(expand=True, fill='both')
            
            def update_text():
                for line in process.stdout:
                    text_area.insert(tk.END, line)
                    text_area.see(tk.END)
                    text_area.update()
                for line in process.stderr:
                    text_area.insert(tk.END, line)
                    text_area.see(tk.END)
                    text_area.update()
            
            import threading
            threading.Thread(target=update_text, daemon=True).start()

        except Exception as e:
            messagebox.showerror("Error", str(e))
    
    def select_output_directory(self):
        directory = filedialog.askdirectory()
        if directory:
            self.output_dir_var.set(directory)
    
    def select_clip_path(self):
        if self.check.get():
            filepath = filedialog.askopenfilename(filetypes=[("Shapefile", "*.shp")])
            self.roi.configure(state="normal")
            self.roi.delete(0, tk.END)
            self.roi.insert(0, filepath)
            self.roi.configure(state="disabled")
        else:
            self.roi.configure(state="normal")
            self.roi.delete(0, tk.END)

    def open_calendar(self, date_type):
        top = ctk.CTkToplevel(self)
        top.title("Select Date")
        cal_frame = ctk.CTkFrame(top)
        cal_frame.pack(padx=10, pady=10)

        year = ctk.CTkComboBox(cal_frame, values=list(map(str, range(datetime.now().year, 2015, -1))))
        year.set(datetime.now().year)
        year.grid(row=0, column=0, padx=5)

        month = ctk.CTkComboBox(cal_frame, values=[f"{i:02d}" for i in range(1, 13)])
        month.set(datetime.now().month)
        month.grid(row=0, column=1, padx=5)

        day = ctk.CTkComboBox(cal_frame, values=[f"{i:02d}" for i in range(1, 32)])
        day.set(datetime.now().day)
        day.grid(row=0, column=2, padx=5)
        
        top.wm_transient(self)

        def set_date():
            selected_date = f"{year.get()}-{month.get()}-{day.get()}"
            if date_type == 'start':
                self.start_date_var.set(selected_date)
            else:
                self.end_date_var.set(selected_date)
            top.destroy()

        ctk.CTkButton(cal_frame, text="Select", command=set_date).grid(row=1, column=1, pady=10)


# ------------------------------------------------------------------------------
# MENÚ LATERAL
# ------------------------------------------------------------------------------
class Menu(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master)
        # Ajustamos la rejilla para que haya espacio
        self.grid_columnconfigure([i for i in range(4)], weight=1)
        self.grid_rowconfigure([i for i in range(8)], weight=1)
        
        self.selected_usos = []
        self.clip = None
        self.directory = ""
        
        self.usos_open = False  # Para controlar si el frame de usos está desplegado
        
        self.createWidgets()
    
    def setConfig(self):
        global user_sql_url, ogr_path
        conf = open('./config/config.txt', 'w')
        user_sql_url = simpledialog.askstring('SQL user URL', 'Write the database url for the features extraction')
        ogr_path = simpledialog.askstring('OGR Path', 'Write the OGR path')
        text = f"{user_sql_url}\n{ogr_path}"
        conf.write(text)
    
    def createWidgets(self):
        # Botón Set Configuration
        self.btn_config = ctk.CTkButton(
            master=self, 
            width=80, 
            text="Set Configuration", 
            font=("Helvetica", 15, "bold"), 
            command=self.setConfig
        )
        self.btn_config.grid(row=0, column=0, pady=6, padx=40, sticky="we")
        
        # Botón "Select Directory"
        self.btn_dir = ctk.CTkButton(
            master=self, 
            width=160, 
            text="Select Directory", 
            font=("Helvetica", 15, "bold"), 
            command=self.selectdir
        )
        self.btn_dir.grid(row=1, column=0, sticky="we", padx=40)
        
        self.entry_dir = ctk.CTkEntry(self, placeholder_text="No directory selected")
        self.entry_dir.grid(row=1, column=1, sticky="we", columnspan=3, padx=10)
        self.entry_dir.configure(state="disabled")
        
        # Provincia
        self.prov_label = ctk.CTkLabel(self, text="Select Province", font=('Helvetica', 15, "bold"))
        self.prov_label.grid(row=2, column=0, sticky="we", padx=40)
        
        self.provincias = AutocompleteCombobox(
            self, 
            completevalues=provincias_españa, 
            height=10, 
            font=('Helvetica', 15)
        )
        self.provincias.set("Select province")
        self.provincias.grid(row=2, column=1, sticky="we", padx=10, columnspan=3)
        self.provincias.set("")
        
        # ROI
        self.clipLabel = ctk.CTkButton(
            self,
            text="Select ROI (optional)",
            font=('Helvetica', 15, "bold"),
            command=self.selectclip
        )
        self.clipLabel.grid(row=3, column=0, sticky="we", padx=40)
        
        self.entry_clip = ctk.CTkEntry(self, placeholder_text="No file selected")
        self.entry_clip.grid(row=3, column=1, sticky="we", columnspan=3, padx=10)
        self.entry_clip.configure(state="disabled")
        
        # DESPLEGABLE PARA USOS DEL SUELO (checkboxes con scroll)
        self.usos_button = ctk.CTkButton(
            self,
            text="▼  Select Land Uses",
            font=("Helvetica", 15, "bold"),
            command=self.toggle_usos
        )
        self.usos_button.grid(row=4, column=0, sticky="we", padx=40, columnspan=4, pady=(5,5))
        
        # Frame oculto con scroll y checkboxes
        self.usos_frame = ctk.CTkScrollableFrame(self, width=320, height=120, label_text="Select Land Uses:")
        self.usos_frame.grid(row=5, column=0, columnspan=4, sticky="nwe", padx=40, pady=5)
        self.usos_frame.grid_remove()  # lo ocultamos de inicio
        
        self.usos_vars = {}
        for code, desc in usos_suelo.items():
            var = tk.BooleanVar(value=False)
            chk = ctk.CTkCheckBox(
                master=self.usos_frame,
                text=f"{code} - {desc}",
                variable=var
            )
            chk.pack(anchor="w", pady=2, padx=5)
            self.usos_vars[code] = var
        
        # Intervalo de años
        self.start_years = self.generate_years(2020, 2024)
        self.end_years = self.generate_years(2020, 2024)
        
        self.yearsLabel = ctk.CTkLabel(self, text="Year Interval", font=('Helvetica', 15, "bold"), width=20)
        self.yearsLabel.grid(row=6, column=0, sticky="we", padx=10)
        
        self.start_year_selector = ctk.CTkComboBox(self, values=self.start_years, font=('Helvetica', 15), width=180)
        self.start_year_selector.set("INITIAL YEAR")
        self.start_year_selector.grid(row=6, column=1, sticky="we", padx=10)
        
        self.end_year_selector = ctk.CTkComboBox(self, values=self.end_years, font=('Helvetica', 15), width=180)
        self.end_year_selector.set("END YEAR")
        self.end_year_selector.grid(row=6, column=2, sticky="we", padx=10)
        
        # Barra de progreso
        self.progressbar = ctk.CTkProgressBar(self, orientation="horizontal", determinate_speed=.5, mode="determinate", height=12)
        self.progressbar.grid(row=7, column=1, sticky="we", padx=10, columnspan=3)
        self.progressbar.set(0)
        
        self.porcentaje = ctk.CTkLabel(self, text="0%", font=('Helvetica', 14))
        self.porcentaje.grid(row=7, column=0, sticky="e", padx=10)
        
        # Botón Start Process
        self.start_process = ctk.CTkButton(
            self,
            width=80,
            text="Start Process",
            command=self.startProcess,
            fg_color="Blue",
            font=('Helvetica', 18, "bold")
        )
        self.start_process.grid(row=8, column=1, sticky="we", padx=10, pady=15, columnspan=2)
    
    def toggle_usos(self):
        """Muestra/oculta el frame con checkboxes de usos."""
        if self.usos_open:
            # Ocultamos el frame
            self.usos_frame.grid_remove()
            # Actualizamos el texto del botón
            self.usos_button.configure(text="▼  Select Land Uses")
            self.usos_open = False
            # Guardar selección en self.selected_usos
            self.selected_usos.clear()
            for code, var in self.usos_vars.items():
                if var.get():
                    self.selected_usos.append(code)
            print("Usos seleccionados:", self.selected_usos)
        else:
            # Mostramos el frame
            self.usos_frame.grid()
            self.usos_button.configure(text="▲  Select Land Uses")
            self.usos_open = True
    
    def selectdir(self):
        self.directory = filedialog.askdirectory(title="Select Directory")
        self.entry_dir.configure(state="normal")
        self.entry_dir.delete(0, tk.END)
        self.entry_dir.insert(0, str(self.directory))
        self.entry_dir.configure(state="disabled")
    
    def selectclip(self):
        self.clip = filedialog.askopenfilename(title="Select File", filetypes=(('ShapeFile', '.shp'),('All files','*.*')))
        self.entry_clip.configure(state="normal")
        self.entry_clip.delete(0, tk.END)
        self.entry_clip.insert(0, self.clip)
        self.entry_clip.configure(state="disabled")
    
    def generate_years(self, start, end):
        return [str(year) for year in range(start, end + 1)]
    
    def startProcess(self):
        global user_sql_url, ogr_path
        
        # Si el dropdown de usos estaba abierto, lo cerramos para asegurar la recogida final
        if self.usos_open:
            self.toggle_usos()
        
        if user_sql_url:
            if (self.start_year_selector.get() != "INITIAL YEAR" and
                self.end_year_selector.get() != "END YEAR" and
                self.directory != "" and
                self.provincias.get() != ""):
                
                if self.start_year_selector.get() >= self.end_year_selector.get():
                    messagebox.showwarning('Year Selector', 'The end year must be bigger than the initial year')
                    return
                
                self.progressbar.set(0)
                self.porcentaje.configure(text='0%')
                
                from __main__ import FEGA_REC_APP
                t = Thread(
                    target=FEGA_REC_APP.main,
                    args=(
                        int(self.start_year_selector.get()),
                        int(self.end_year_selector.get()),
                        str(self.directory),
                        str(provincias_sencillas[self.provincias.get()]),
                        str(user_sql_url),
                        self.clip,
                        str(ogr_path),
                        self.selected_usos
                    )
                )
                t.start()
                self.check_thread(t)
            else:
                messagebox.showwarning('Atribute not set', 'Select all the parameters to proceed')
        else:
            messagebox.showerror('Configuration alert', 'You must set the configuration path before starting the process')
    
    def check_thread(self, thread):
        from __main__ import FEGA_REC_APP
        if thread.is_alive():
            self.progressbar.set(FEGA_REC_APP.percentaje / 100)
            self.porcentaje.configure(text=f'{FEGA_REC_APP.message} - {round(FEGA_REC_APP.percentaje,1)}%')
            self.after(500, lambda: self.check_thread(thread))
        else:
            self.progressbar.set(FEGA_REC_APP.percentaje / 100)
            self.porcentaje.configure(text=f'{FEGA_REC_APP.message} - {FEGA_REC_APP.percentaje}%')
            if FEGA_REC_APP.percentaje != 100:
                messagebox.showerror('ERROR', 'The process has failed')
            else:
                messagebox.showinfo('Process completed', f'Data stored at: {FEGA_REC_APP.outpath}')
    
    def borrar(self):
        self.provincias.set("")


# ------------------------------------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    # Leemos config.txt si existe
    if os.path.exists('./config/config.txt'):
        with open('./config/config.txt') as fd:
            lines = fd.readlines()
            if len(lines) == 2:
                user_sql_url = lines[0].strip()
                ogr_path = lines[1].strip()

    app = Fega()
    app.mainloop()
