import geopandas as gpd
from tqdm import tqdm
from datetime import date
from sqlalchemy import create_engine
import os
from shapely.geometry import MultiPolygon
import shapely
from shapely.wkt import loads, dumps 
import subprocess
import numpy as np 
import pandas as pd
import warnings
import sys
from shapely.geometry import box, LineString
from scipy.spatial import ConvexHull

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
    "OP": "Otros cultivos Permanentes", 
}



# import arcpy 
warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)
warnings.filterwarnings("ignore", message="`unary_union` returned None due to all-None GeoSeries")

message = ''
percentaje = 0


def config_csv(csv_path: str) -> dict:
    """
    Reads a CSV file containing configuration data for comunidades autónomas, including start/end dates
    and database names, and converts it into a dictionary.

    Args:
        csv_path (str): Path to the CSV configuration file.

    Returns:
        dict: A dictionary with comunidad autónoma as keys and tuples containing database name
              and start/end dates for each year as values.
    """
    # Load the CSV with the appropriate delimiter
    df = pd.read_csv(csv_path, delimiter=';')
    
    # Filter relevant columns
    years = [str(i) for i in range(2020, 2024 + 1)]
    relevant_columns = ['Comunidad_autonoma', 'Nombre_base_datos'] + years
    df = df[relevant_columns]
    
    # Create configuration dictionary
    config = {}
    for _, row in df.iterrows():
        comunidad = row['Comunidad_autonoma']
        database_name = row['Nombre_base_datos']
        dates = {year: row[year] for year in years}
        config[comunidad] = (database_name, dates)
    
    return config

# Crear el diccionario de provincias con nombres de comunidades del CSV
def create_prov_dict(config):
    global prov
    prov = {
        'Alava': (1, config['PAIS-VASCO']),
        'Albacete': (2, config['CASTILLA-LA-MANCHA']),
        'Alicante': (3, config['VALENCIANA']),
        'Almeria': (4, config['ANDALUCÍA']),
        'Avila': (5, config['CASTILLA-Y-LEÓN']),
        'Badajoz': (6, config['EXTREMADURA']),
        'Baleares': (7, config['BALEARES']),
        'Barcelona': (8, config['CATALUÑA']),
        'Burgos': (9, config['CASTILLA-Y-LEÓN']),
        'Caceres': (10, config['EXTREMADURA']),
        'Cadiz': (11, config['ANDALUCÍA']),
        'Castellon': (12, config['VALENCIANA']),
        'CiudadReal': (13, config['CASTILLA-LA-MANCHA']),
        'Cordoba': (14, config['ANDALUCÍA']),
        'Coruña': (15, config['GALICIA']),
        'Cuenca': (16, config['CASTILLA-LA-MANCHA']),
        'Girona': (17, config['CATALUÑA']),
        'Granada': (18, config['ANDALUCÍA']),
        'Guadalajara': (19, config['CASTILLA-LA-MANCHA']),
        'Guipuzcoa': (20, config['PAIS-VASCO']),
        'Huelva': (21, config['ANDALUCÍA']),
        'Huesca': (22, config['ARAGÓN']),
        'Jaen': (23, config['ANDALUCÍA']),
        'Leon': (24, config['CASTILLA-Y-LEÓN']),
        'Lleida': (25, config['CATALUÑA']),
        'LaRioja': (26, config['LA-RIOJA']),
        'Lugo': (27, config['GALICIA']),
        'Madrid': (28, config['MADRID']),
        'Malaga': (29, config['ANDALUCÍA']),
        'Murcia': (30, config['MURCIA']),
        'Navarra': (31, config['NAVARRA']),
        'Ourense': (32, config['GALICIA']),
        'Asturias': (33, config['ASTURIAS']),
        'Palencia': (34, config['CASTILLA-Y-LEÓN']),
        'LasPalmas': (35, config['CANARIAS']),
        'Pontevedra': (36, config['GALICIA']),
        'Salamanca': (37, config['CASTILLA-Y-LEÓN']),
        'Tenerife': (38, config['CANARIAS']),
        'Cantabria': (39, config['CANTABRIA']),
        'Segovia': (40, config['CASTILLA-Y-LEÓN']),
        'Sevilla': (41, config['ANDALUCÍA']),
        'Soria': (42, config['CASTILLA-Y-LEÓN']),
        'Tarragona': (43, config['CATALUÑA']),
        'Teruel': (44, config['ARAGÓN']),
        'Toledo': (45, config['CASTILLA-LA-MANCHA']),
        'Valencia': (46, config['VALENCIANA']),
        'Valladolid': (47, config['CASTILLA-Y-LEÓN']),
        'Vizcaya': (48, config['PAIS-VASCO']),
        'Zamora': (49, config['CASTILLA-Y-LEÓN']),
        'Zaragoza': (50, config['ARAGÓN']),
    }
    return prov


def load_rd(gpkg_path:str) -> list:
    """## LOAD rd
    Given the path of the gpkg file, returns a list of the layers in the gpkg file.

    Args:
        gpkg_path (str): gpkg path

    Returns:
        list: Geodataframe list
    """
    gpkgs = []
    for year in range(int(ini), int(fin)+1):
        gpkgs.append(f'rd_{num_prov}_{year}')
    return gpkgs

def recintos():
    global percentaje
    rc_df = []
    dif = fin+1-ini
    it = 20/dif
    # usos_seleccionados = tuple(usos_seleccionados)
    for año in tqdm(range(int(ini), int(fin)+1)):
        año_s = str(año)
        año_f = str(año+1)
        fecha_inicio = date.fromisoformat(prov[provincia][1][1][año_s])
        try:
            fecha_fin = date.fromisoformat(prov[provincia][1][1][año_f])
        except:
            fecha_fin = f'{año_f}-12-31'
        tqdm.write("Extrayendo la tabla rc_"+ str(provincia)+"_"+str(año)+ "...")
        query = f"""SELECT
    DISTINCT(dn_oid),
    ST_MakeValid(ST_TRANSFORM(dn_geom,32630),'method=structure keepcollapsed=false') as dn_geom,
    dn_initialdate,
    dn_enddate,
    cap_prevalente as cp_{año},
    uso_sigpac as uso_sigpac_{año},
    incidencias as incidencias_{año},
    concat(provincia,'-',municipio,'-', agregado,'-',zona,'-',poligono,'-',parcela,'-',recinto) as id_recinto_{año}, 
    ST_AREA(ST_TRANSFORM(dn_geom,32630)) as area_m2
FROM t$recinto_ex
WHERE 
    (dn_initialdate <= '{fecha_fin}' AND ((dn_enddate BETWEEN '{fecha_inicio}' AND '{fecha_fin}') or dn_enddate is null)) and
    (dn_initialdate_1 <= '{fecha_fin}' AND ((dn_enddate_1 BETWEEN '{fecha_inicio}' AND '{fecha_fin}') or dn_enddate_1 is null))
    AND provincia = {num_prov} 
    AND uso_sigpac <> 'CA' AND uso_sigpac <> 'AG' AND uso_sigpac <> 'ZU' AND uso_sigpac <> 'ED' AND uso_sigpac <> 'ZC' AND uso_sigpac <> 'ZV' AND uso_sigpac <> 'IV'
    AND ST_AREA(ST_TRANSFORM(dn_geom,32630)) > 5000 
    
"""
        try:
            rc_df.append(gpd.GeoDataFrame.from_postgis(query, sql_engine, geom_col='dn_geom'))

        # gpkgs.add(dir)
            rc_df[-1] = rc_df[-1][rc_df[-1]['dn_geom'].notna()]
            # rc_df[-1] = shapely.make_valid(rc_df[-1].geometry)
            rc_df[-1]['dn_initialdate'] = rc_df[-1]['dn_initialdate'].astype(str)
            rc_df[-1]['dn_enddate'] = rc_df[-1]['dn_enddate'].astype(str)
            
        except Exception as e:
            tqdm.write("Ya existe la tabla rd_"+str(provincia) + "_" + str(año)+ "..."+ str(e))
        percentaje += it
    return rc_df

def lineas():
    global percentaje
    ld_df = []
    if ini in [2018, 2019]:
        #### Take the input directory for 2018
        ld_18 = input("Introduce el directorio de entrada de las lineas de declaracion del 18: ")
        ld_19 = input("Introduce el directorio de entrada de las lineas de declaracion del 19: ")
        ld_df.append(gpd.read_file(ld_18, layer = f'LD_{provincia}_2018'))
        ld_df.append(gpd.read_file(ld_19, layer = f'LD_{provincia}_2019'))
    dif = fin+1-ini
    it = 20/dif    
    for año in tqdm(range(int(ini), int(fin)+1)):
        año_s = str(año)
        fecha_inicio = date.fromisoformat(año_s+'-01-01')
        fecha_fin = date.fromisoformat(año_s+'-12-31')
        if año in [2018, 2019]:
            pass
            
        else:
            tqdm.write("Extrayendo la tabla ld_"+ str(provincia)+"_"+str(año)+ "...")
            query = f"""
SELECT         
    ld.dn_pk as ld_pk_{año},
    ld.parc_producto as parc_producto_{año},
    ld.dn_oid,
    ld.dn_surface,
    ld.dn_initialdate,
    ld.dn_enddate,
    ST_MakeValid(ST_TRANSFORM(dn_geom,32630),'method=structure keepcollapsed=false') as dn_geom,
    ST_AREA(ST_TRANSFORM(ld.dn_geom,32630)) as area_m2,
    concat(provincia,'-',municipio,'-', agregado,'-',zona,'-',poligono,'-',parcela,'-',recinto) as id_recinto_lineas_{año}
    FROM t$linea_declaracion AS ld
WHERE 
    ((ld.dn_initialdate <= '{fecha_fin}' AND (ld.dn_enddate BETWEEN '{fecha_inicio}' AND '{fecha_fin}') OR ld.dn_enddate IS NULL) 
    AND ld.provincia = {num_prov}) AND (ST_AREA(ST_TRANSFORM(ld.dn_geom,32630)) > 5000)
"""
            try:
                ld_df.append(gpd.GeoDataFrame.from_postgis(query, sql_engine, geom_col='dn_geom'))
                

            # gpkgs.add(dir)
                ld_df[-1] = ld_df[-1][ld_df[-1]['dn_geom'].notna()]
                ld_df[-1]['dn_initialdate'] = ld_df[-1]['dn_initialdate'].astype(str)
                ld_df[-1]['dn_enddate'] =ld_df[-1]['dn_enddate'].astype(str)
                
            except Exception as e:
                tqdm.write("Ya existe la tabla rd_"+str(provincia) + "_" + str(año)+ "..."+ str(e))
        percentaje += it
    return ld_df

def corregir_geometrias(gdf):
    # Evita modificar el GeoDataFrame original
    gdf = gdf.copy(deep=True)
    
    # Elimina geometrías nulas y vacías
    gdf = gdf[gdf.geometry.notna()]
    gdf = gdf[~gdf.geometry.is_empty]
    
    def corregir_geometria(geom):
        if not geom.is_valid:
            geom = shapely.make_valid(geom)
        
        # Fuerza 2D si es necesario
        geom = shapely.force_2d(geom)
        
        # Simplifica la geometría
        geom = shapely.simplify(geom, tolerance=0.1)
        
        # Establece la precisión
        geom = shapely.set_precision(geom, grid_size=0.1)
        
        # Intenta resolver intersecciones no nodales
        if geom.geom_type in ['MultiPolygon', 'MultiLineString']:
            geom = shapely.ops.unary_union(geom)
            if geom.geom_type == 'GeometryCollection':
                geom = shapely.geometry.MultiPolygon([g for g in geom.geoms if isinstance(g, (shapely.geometry.Polygon, shapely.geometry.MultiPolygon))])
        
        geom = shapely.set_precision(geom, grid_size=2)
        return geom

    # Aplica las correcciones a todas las geometrías
    gdf.geometry = gdf.geometry.apply(corregir_geometria)
    
    return gdf

def filiformes(input_gdf):
    """
    Identifies filiform (elongated) geometries in a GeoDataFrame
    
    Args:
        input_gdf (GeoDataFrame): Input GeoDataFrame
        
    Returns:
        GeoDataFrame: Filtered GeoDataFrame containing only eligible geometries
    """
    # Ensure we're working with a GeoDataFrame
    if not isinstance(input_gdf, gpd.GeoDataFrame):
        try:
            input_gdf = gpd.GeoDataFrame(input_gdf)
        except:
            raise TypeError("Input must be convertible to a GeoDataFrame")
    
    # Make a copy to avoid modifying the original
    gdf = input_gdf.copy()
    gdf['perimeter_area_ratio'] = gdf.geometry.length / gdf.geometry.area
    
    # Filter eligible geometries
    mask = gdf['perimeter_area_ratio'] <= 4

    gdf = gdf.drop(columns=['perimeter_area_ratio'])
    print(f"Identified {mask.sum()} filiform geometries")
    return gdf[mask].reset_index(drop=True)

def eliminate_overlaps_geodataframe(gdf):
    """
    Eliminates overlapping geometries in a GeoDataFrame, keeping the one with larger area.

    Args:
        gdf (GeoDataFrame): Input GeoDataFrame with potentially overlapping geometries

    Returns:
        GeoDataFrame: Clean GeoDataFrame with overlaps removed
    """
    # Make a copy to avoid modifying the original
    gdf = gdf.copy()

    # Ensure we're working with a GeoDataFrame
    if not isinstance(gdf, gpd.GeoDataFrame):
        raise TypeError("Input must be a GeoDataFrame")

    # Calculate area of each geometry
    gdf['area'] = gdf.geometry.area

    # List to store indices of geometries to delete
    to_delete = []

    # Create spatial index for faster processing
    sindex = gdf.sindex

    # Iterate through each geometry
    for idx1, geom1 in enumerate(gdf.geometry):
        area1 = gdf.iloc[idx1].area

        # Get potential overlapping geometries using spatial index
        possible_matches_idx = list(sindex.intersection(geom1.bounds))

        # Compare with other geometries
        for idx2 in possible_matches_idx:
            if idx2 != idx1:
                geom2 = gdf.geometry.iloc[idx2]
                area2 = gdf.iloc[idx2].area

                # Check for intersection
                if geom1.intersects(geom2):
                    try:
                        intersection_area = geom1.intersection(geom2).area
                        # If overlap is significant (more than 1% of smaller area)
                        if intersection_area > min(area1, area2) * 0.01:
                            # Keep the larger geometry
                            if area1 > area2:
                                to_delete.append(idx2)
                            else:
                                to_delete.append(idx1)
                                break
                    except Exception as e:
                        print(f"Warning: Failed to process intersection for geometries {idx1} and {idx2}: {e}")
                        continue

    # Remove overlapping geometries
    gdf = gdf.drop(index=to_delete).reset_index(drop=True)

    return gdf


def overlay_rd(recintos_df, lineas_df, outdir = ''):
    global percentaje
    overlay_type = 'union'
    rd_dir = os.path.join(outdir, f"{roi}.gpkg")
    cur_year = ini
    layers = []
    it = 20/len(recintos_df)
    for i in tqdm(range(len(recintos_df))):
        
        ##################################################

        rc = gpd.GeoDataFrame(recintos_df[i], geometry='dn_geom', crs=32630)
        
        ## erase the areas that are less than 4 hectares
        rc = corregir_geometrias(rc)
        rc = filiformes(rc)
        rc = eliminate_overlaps_geodataframe(rc)
        ##################################################
              
        ld = gpd.GeoDataFrame(lineas_df[i], geometry='dn_geom', crs=32630)
        ld = corregir_geometrias(ld)
        ld = filiformes(ld)
        ld = eliminate_overlaps_geodataframe(ld)
        ##################################################
        try:
            over_df = gpd.overlay(rc, ld, how=overlay_type, make_valid=True, keep_geom_type=True)
        except:
            over_df = gpd.overlay(rc, ld, how='intersection', keep_geom_type=True)
        # over_df = eliminate_overlaps_geodataframe(over_df)
        over_df = filiformes(over_df)
        over_df = corregir_geometrias(over_df)
        over_df = over_df.drop_duplicates(subset='geometry')

        ####################################################
        
        campos_export = [f'uso_sigpac_{cur_year}', f'parc_producto_{cur_year}', f'incidencias_{cur_year}',f'cp_{cur_year}', f'id_recinto_{cur_year}'
                         ,'geometry']
        over_df = over_df[campos_export]
        over_df[f'incidencias_{cur_year}'] = over_df[f'incidencias_{cur_year}'].astype(str)
        over_df = gpd.GeoDataFrame(over_df, geometry='geometry', crs=recintos_df[i].crs)
        
        over_df[f'est_aband_{cur_year}'] =over_df.apply(lambda row:
        'SLD' if pd.isna(row[f'parc_producto_{cur_year}']) or row[f'parc_producto_{cur_year}'] == 0 else 
        'A' if '117' in row[f'incidencias_{cur_year}'] or '199' in row[f'incidencias_{cur_year}'] else 
        'M5' if '177' in row[f'incidencias_{cur_year}'] or '186' in row[f'incidencias_{cur_year}'] else
        'NA' if row[f'uso_sigpac_{cur_year}'] == 'OV' and row[f'parc_producto_{cur_year}'] == 101 else
        'PA' if row[f'uso_sigpac_{cur_year}'] == 'OV' and row[f'parc_producto_{cur_year}'] != 101 else
        'NA_150' if row[f'uso_sigpac_{cur_year}'] == 'OV' and row[f'parc_producto_{cur_year}'] == 150 else
        'SRC' if pd.isna(row[f'uso_sigpac_{cur_year}']) else 
        
        'NA' if row[f'uso_sigpac_{cur_year}'] == 'FY' and (
            row[f'parc_producto_{cur_year}'] in [104,105,106,107,108,109,110,111,112,113] or

            row[f'parc_producto_{cur_year}'] in [200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218] or

            row[f'parc_producto_{cur_year}'] in [300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,
                                                 318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,
                                                 340,341,342,343,344,345,346,347,348,349]
        )
        else
        'NA_150' if row[f'uso_sigpac_{cur_year}'] == 'FY' and row[f'parc_producto_{cur_year}'] == 150 else
        # 'NA' if row[f'uso_sigpac_{cur_year}'] in ['PR', 'PS', 'PA'] and 61 <= row[f'parc_producto_{cur_year}'] <= 65 else
        'NA' if row[f'uso_sigpac_{cur_year}'] in ['PR', 'PS', 'PA'] and row[f'parc_producto_{cur_year}'] in [61, 62, 63, 64, 65] else
        'PA' if row[f'uso_sigpac_{cur_year}'] in ['PR', 'PS', 'PA'] and 61 > row[f'parc_producto_{cur_year}'] or row[f'parc_producto_{cur_year}'] > 65 else        
        'NA_150' if row[f'uso_sigpac_{cur_year}'] in ['PR', 'PS', 'PA'] and row[f'parc_producto_{cur_year}'] == 150 else
        'Uso_alt'
        ,axis=1)
        ## Transform all the geometries to multipolygon
        over_df['geometry'] = over_df['geometry'].apply(lambda x: MultiPolygon([x]) if x.geom_type ==  'Polygon' else x)
        
        over_df.to_file(rd_dir, driver='GPKG', layer=f'rd_{num_prov}_{cur_year}')
        layers.append(f'rd_{num_prov}_{cur_year}')
        cur_year = str(int(cur_year) + 1)
        percentaje += it
    return rd_dir, layers
  


def overlay_crono(fois_clip, crono_gdb): 
    global percentaje
    overlayType = "union"
    years = [str(i) for i in range(int(ini)+1, int(fin)+1)]
    it = 20/len(years)
    overlay_layer = None
    input_layer = fois_clip[0]
    for year in years:
        output_layer = input_layer + f'_{year}'
        current_layer = f'rd_{roi}_{year}'
        
        try:
            tqdm.write(f"Overlay started, {input_layer}, {current_layer}, {output_layer}")

            ### CURRENT LAYER
            
            cur_df = gpd.GeoDataFrame.from_file(crono_gdb, layer = current_layer)
            cur_df = filiformes(cur_df)
            cur_df = corregir_geometrias(cur_df)
            cur_df = cur_df[cur_df.geom_type.isin(['Polygon','MultiPolygon'])]

           
            ### Input layer
            
            in_df = gpd.GeoDataFrame.from_file(crono_gdb, layer = input_layer)

            in_df = filiformes(in_df)
            in_df = corregir_geometrias(in_df)

            in_df = in_df[in_df.geom_type.isin(['Polygon', 'MultiPolygon'])]
  
            
            #######
            
            ### OVERLAY
            try:
                over_df = gpd.overlay(in_df, cur_df, how=overlayType,  make_valid=True, keep_geom_type=True)
                over_df = corregir_geometrias(over_df)
                over_df = filiformes(over_df)
                over_df = over_df[over_df.geom_type.isin(['Polygon', 'MultiPolygon'])]
            except:
                over_df = gpd.overlay(in_df, cur_df, how='intersection', keep_geom_type=True)
                over_df = corregir_geometrias(over_df)
                over_df = filiformes(over_df)
                over_df = over_df[over_df.geom_type.isin(['Polygon', 'MultiPolygon'])]
            
            
            over_df = gpd.GeoDataFrame(over_df, geometry='geometry', crs=32630)
            # Añade una columna para indicar si un polígono está contenido dentro de otro
            over_df.to_file(crono_gdb, driver='GPKG', layer=output_layer)

            tqdm.write(f"Saved overlay to {crono_gdb} as {output_layer}")   
        except Exception as e:
            tqdm.write(f"Error overlaying: {e}")
        input_layer = output_layer
        percentaje += it
    return output_layer  

def campos(overlay_layer,crono_gdb):
    global ini, fin, roi
    overlay_layer = gpd.GeoDataFrame.from_file(crono_gdb, layer = overlay_layer)
    def calcular_acrono(row, years):
        resultado = ''
        for year in years:
            aband = row[f'est_aband_{year}']
            if aband == 'A':
                resultado += '0'
            elif aband == 'NA':
                resultado += '1'
            elif aband == 'SLD':
                resultado += '2'
            elif aband == 'SRC':
                resultado += '3'
            elif aband == 'PA':
                resultado += '4'
            elif  aband == 'None':
                resultado += '9'
            else:
                resultado += '5'
                
        return resultado
    
    def calcular_pcrono(row,years):
        resultado = ''
        
        for year in years:
            resultado += '_'
            prod = str(int(0 if np.isnan(row[f'parc_producto_{year}'])else row[f'parc_producto_{year}']))
            resultado += prod
            
        return resultado[1:]
    
    def calcular_ucrono (row,years):
        resultado = ''
        for year in years:
            resultado += '_'
            uso = str('NU' if pd.isna(row[f'uso_sigpac_{year}']) else row[f'uso_sigpac_{year}'])
            resultado += uso
        return resultado[1:]
    
    def calcular_icrono(row,years):
        resultado = ''
        for year in years:
            resultado += '_'
            incid = str('0' if pd.isna(row[f'incidencias_{year}']) else row[f'incidencias_{year}'])
            resultado += incid
        return resultado[1:]


    ini = int(ini)
    fin = int(fin)
    
    overlay_layer['a_crono'] = overlay_layer.apply(lambda row: calcular_acrono(row, range(ini, fin+1)), axis=1)
    overlay_layer['p_crono'] = overlay_layer.apply(lambda row: calcular_pcrono(row, range(ini, fin+1)), axis=1)
    overlay_layer['u_crono'] = overlay_layer.apply(lambda row: calcular_ucrono(row, range(ini, fin+1)), axis=1)
    overlay_layer['i_crono'] = overlay_layer.apply(lambda row: calcular_icrono(row, range(ini, fin+1)), axis=1)
    ### take the last id recinto wich is not null 
    overlay_layer['id_recinto'] = overlay_layer[[f'id_recinto_{year}' for year in range(ini, fin+1)]].bfill(axis=1).iloc[:, 0]
    

    campos_export = ['geometry', 'a_crono', 'p_crono', 'u_crono','i_crono', 'id_recinto']
    crono_fin = overlay_layer[campos_export]
    crono_fin = crono_fin.drop_duplicates(subset='geometry')  

    # Filter using all selected usos
    usos_pattern = '|'.join(usos_seleccionados)
    print(usos_pattern)
    crono_fin_filtered = crono_fin[crono_fin['u_crono'].str.contains(usos_pattern)]
    crono_fin_filtered.to_file(crono_gdb, driver='GPKG', layer=f'CRONO_FIN_{roi}')
    if not usos_seleccionados == list(usos_suelo.keys()):
        for uso in usos_seleccionados:
            crono_fin_uso = crono_fin[crono_fin['u_crono'].str.contains(uso)]
            crono_fin_uso.to_file(crono_gdb, driver='GPKG', layer=f'CRONO_{roi}_{uso}')
        

def clip(list_dfs, clip):
    for i in range(len(list_dfs)):
        list_dfs[i] = gpd.clip(list_dfs[i], clip)
    return list_dfs        


def main(start, end, out_dir, provi, user_url, clip_path = None, usos_sel = ['PS','PA','PR','FY','OV','VI']):
    global ini, fin, num_prov, provincia, sql_engine, percentaje, roi, message, percentaje, outpath, usos_seleccionados
    percentaje = 0
    
    ini = start
    fin = end
    provincia = provi
    outdir = out_dir
    if usos_sel == ["TODOS"]:
        # print(TODOS)
        usos_seleccionados = list(usos_suelo.keys())
    else:
        usos_seleccionados = usos_sel

    config = config_csv(r'./config/CSV_CONFIG.csv')
    prov = create_prov_dict(config)
    
    num_prov = int(prov[provincia][0])
    roi = str(num_prov)
    provincia_num = int(num_prov)
    url = prov[provincia][1][0]
    user = '/'.join([user_url, url])
    sql_engine = create_engine(user)
    # sql_engine = create_engine(f'postgresql://carga:g30qub1dy@localhost:5432/{url}')

    message = 'Extrayendo recintos'
    recintos_df = recintos()
    
    message = 'Extrayendo lineas de declaracion'
    lineas_df = lineas()
    if clip_path:
        cliped = gpd.read_file(clip_path)
        cliped = cliped.to_crs(32630)
        recintos_df = clip(recintos_df, cliped)
        lineas_df = clip(lineas_df, cliped)
    message = 'Creando recintos declarados'
    rd_dir, layers_out = overlay_rd(recintos_df, lineas_df, outdir) 
    fois_clip = layers_out
    crono_gdb = rd_dir
    ### fois clip path 
    # percentaje += 12.5
    message = 'Creando capa CRONO'
    overlay_layer = overlay_crono(fois_clip, crono_gdb)
    overlayed_crono = campos(overlay_layer,crono_gdb)
    message = 'Exportando cronología'
    percentaje = 100
    message = 'Proceso finalizado'
    outpath = crono_gdb

if __name__ == '__main__':
    main()