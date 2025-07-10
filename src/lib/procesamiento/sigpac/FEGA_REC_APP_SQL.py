import os
import pandas as pd
from sqlalchemy import create_engine, text

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

def config_csv(csv_path: str) -> dict:
    df = pd.read_csv(csv_path, delimiter=';')
    years = [str(i) for i in range(2020, 2025)]
    relevant_columns = ['Comunidad_autonoma', 'Nombre_base_datos'] + years
    df = df[relevant_columns]
    config = {}
    for _, row in df.iterrows():
        comunidad = row['Comunidad_autonoma']
        database_name = row['Nombre_base_datos']
        dates = {year: row[year] for year in years}
        config[comunidad] = (database_name, dates)
    return config

def create_prov_dict(config):
    return {
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

def recintos(conn, ini, fin, num_prov, prov_dates):
    tables = []
    for year in range(ini, fin+1):
        tname = f"recintos_filtrados_{num_prov}_{year}"
        inicio = prov_dates[str(year)]
        fin_alt = prov_dates.get(str(year+1), f"{year+1}-12-31")
        sql = f"""
        DROP TABLE IF EXISTS {tname};
        CREATE TABLE {tname} AS
        SELECT DISTINCT
          dn_oid,
          ST_MakeValid(ST_Transform(dn_geom,32630)) AS geom,
          cap_prevalente    AS cp_{year},
          uso_sigpac        AS uso_sigpac_{year},
          incidencias       AS incidencias_{year},
          ST_Area(ST_Transform(dn_geom,32630)) AS area_m2_{year},
          CONCAT(provincia,'-',municipio,'-',agregado,'-',zona,'-',poligono,'-',parcela,'-',recinto) AS id_recinto_{year}
        FROM t$recinto_ex
        WHERE provincia = {num_prov}
          AND dn_initialdate <= '{fin_alt}'
          AND (dn_enddate BETWEEN '{inicio}' AND '{fin_alt}' OR dn_enddate IS NULL)
          AND dn_initialdate_1 <= '{fin_alt}'
          AND (dn_enddate_1 BETWEEN '{inicio}' AND '{fin_alt}' OR dn_enddate_1 IS NULL);
        """
        conn.execute(text(sql))
        tables.append(tname)
    return tables

def lineas(conn, ini, fin, num_prov):
    tables = []
    for year in range(ini, fin+1):
        tname = f"lineas_filtradas_{num_prov}_{year}"
        inicio, fin_alt = f"{year}-01-01", f"{year}-12-31"
        sql = f"""
        DROP TABLE IF EXISTS {tname};
        CREATE TABLE {tname} AS
        SELECT
          dn_pk                         AS ld_pk_{year},
          dn_oid,
          ST_MakeValid(ST_Transform(dn_geom,32630)) AS geom,
          parc_producto                 AS parc_producto_{year},
          dn_initialdate,
          COALESCE(dn_enddate,'{fin_alt}')::date AS dn_enddate
        FROM t$linea_declaracion
        WHERE provincia = {num_prov}
          AND dn_initialdate <= '{fin_alt}'
          AND (dn_enddate BETWEEN '{inicio}' AND '{fin_alt}' OR dn_enddate IS NULL);
        """
        conn.execute(text(sql))
        tables.append(tname)
    return tables

def overlay_rd(conn, rec_t, lin_t, num_prov, out_gpkg, engine_url):
    rd_layers = []
    for t_rec, t_lin in zip(rec_t, lin_t):
        year = t_rec.split('_')[-1]
        rd_table = f"rd_{num_prov}_{year}"
        sql = f"""
        DROP TABLE IF EXISTS {rd_table};
        CREATE TABLE {rd_table} AS
        SELECT
          r.dn_oid,
          l.ld_pk_{year},
          ST_CollectionExtract(
            ST_Multi(ST_Buffer(ST_Intersection(r.geom,l.geom),0)),3) AS geom,
          r.cp_{year}, r.uso_sigpac_{year}, r.incidencias_{year}, r.id_recinto_{year}, r.area_m2_{year},
          CASE
            WHEN l.parc_producto_{year} IS NULL OR l.parc_producto_{year}=0 THEN 'SLD'
            WHEN r.incidencias_{year} ~ '117|199'       THEN 'A'
            WHEN r.incidencias_{year} ~ '177|186'       THEN 'M5'
            WHEN r.uso_sigpac_{year}='OV' AND l.parc_producto_{year}=101 THEN 'NA'
            WHEN r.uso_sigpac_{year}='OV' AND l.parc_producto_{year}<>101 THEN 'PA'
            WHEN r.uso_sigpac_{year}='OV' AND l.parc_producto_{year}=150 THEN 'NA_150'
            WHEN r.uso_sigpac_{year} IS NULL       THEN 'SRC'
            WHEN r.uso_sigpac_{year}='FY' AND l.parc_producto_{year}=150 THEN 'NA_150'
            WHEN r.uso_sigpac_{year}='FY' AND (
                 l.parc_producto_{year} BETWEEN 104 AND 113
              OR l.parc_producto_{year} BETWEEN 200 AND 218
              OR l.parc_producto_{year} BETWEEN 300 AND 349) THEN 'NA'
            WHEN r.uso_sigpac_{year} IN ('PR','PS','PA') AND l.parc_producto_{year} BETWEEN 61 AND 65 THEN 'NA'
            WHEN r.uso_sigpac_{year} IN ('PR','PS','PA') AND (l.parc_producto_{year}<61 OR l.parc_producto_{year}>65) THEN 'PA'
            WHEN r.uso_sigpac_{year} IN ('PR','PS','PA') AND l.parc_producto_{year}=150 THEN 'NA_150'
            ELSE 'Uso_alt'
          END AS est_aband_{year}
        FROM {t_rec} r
        JOIN {t_lin} l ON ST_Intersects(r.geom,l.geom);
        """
        conn.execute(text(sql))
        rd_layers.append(rd_table)
    # Exportar RD a GeoPackage (asegúrate de tener ogr2ogr instalado)
    os.system(
        f'ogr2ogr -f GPKG {out_gpkg} "PG:{engine_url}" {" ".join(rd_layers)}'
    )
    return rd_layers

def overlay_crono(conn, ini, fin, num_prov, usos_sel, out_gpkg, engine_url):
    years = list(range(ini, fin+1))
    base = years[0]
    cte = [
        f"WITH RECURSIVE crono AS (",
        f"  SELECT geom, ARRAY[est_aband_{base}] AS a_crono,",
        f"         ARRAY[parc_producto_{base}] AS p_crono,",
        f"         ARRAY[uso_sigpac_{base}]  AS u_crono,",
        f"         ARRAY[incidencias_{base}] AS i_crono,",
        f"         id_recinto_{base}        AS id_recinto",
        f"  FROM rd_{num_prov}_{base}"
    ]
    for year in years[1:]:
        cte.append("  UNION ALL")
        cte.append(
            f"  SELECT ST_CollectionExtract(ST_Multi(ST_Buffer(ST_Intersection(prev.geom, nxt.geom),0)),3),",
            f" prev.a_crono || nxt.est_aband_{year},",
            f" prev.p_crono || nxt.parc_producto_{year},",
            f" prev.u_crono || nxt.uso_sigpac_{year},",
            f" prev.i_crono || nxt.incidencias_{year},",
            f" prev.id_recinto",
            f" FROM crono prev",
            f" JOIN rd_{num_prov}_{year} nxt",
            f"   ON ST_Intersects(prev.geom, nxt.geom)"
        )
    cte.append(")")
    cte.append(
        f"SELECT geom,",
        f"       array_to_string(a_crono,'' ) AS a_crono,",
        f"       array_to_string(p_crono,'_') AS p_crono,",
        f"       array_to_string(u_crono,'_') AS u_crono,",
        f"       array_to_string(i_crono,'_') AS i_crono,",
        f"       id_recinto",
        f"INTO rd_crono_{num_prov}",
        f"FROM crono",
        f"WHERE cardinality(a_crono) = {len(years)};"
    )
    sql_cte = "\n".join(cte)
    conn.execute(text(sql_cte))

    # Crear CRONO_FIN y por uso
    pattern = '|'.join(usos_sel)
    conn.execute(text(f"DROP TABLE IF EXISTS CRONO_FIN_{num_prov};"))
    conn.execute(text(
        f"CREATE TABLE CRONO_FIN_{num_prov} AS "
        f"SELECT * FROM rd_crono_{num_prov} WHERE u_crono ~ '{pattern}';"
    ))
    extra_layers = [f"CRONO_FIN_{num_prov}"]
    if usos_sel != list(usos_suelo.keys()):
        for uso in usos_sel:
            layer = f"CRONO_{num_prov}_{uso}"
            conn.execute(text(f"DROP TABLE IF EXISTS {layer};"))
            conn.execute(text(
                f"CREATE TABLE {layer} AS "
                f"SELECT * FROM rd_crono_{num_prov} WHERE u_crono ~ '{uso}';"
            ))
            extra_layers.append(layer)

    os.system(
        f'ogr2ogr -f GPKG {out_gpkg} "PG:{engine_url}" {" ".join(extra_layers)}'
    )
    return extra_layers

def main(start, end, out_dir, provi, user_url, clip_path=None, usos_sel=['PS','PA','PR','FY','OV','VI']):
    global ini, fin, num_prov, provincia, roi,percentaje, message
    percentaje = 0
    message = "Iniciando el proceso de generación de GeoPackage..."
    ini = start
    fin = end
    provincia = provi

    if usos_sel == ["TODOS"]:
        usos_seleccionados = list(usos_suelo.keys())
    else:
        usos_seleccionados = usos_sel

    config = config_csv(r'./src/config/CSV_CONFIG.csv')
    prov = create_prov_dict(config)

    num_prov = int(prov[provincia][0])
    roi = str(num_prov)
    url = prov[provincia][1][0]
    user = '/'.join([user_url, url])
    engine = create_engine(user)
    out_gpkg = os.path.join(out_dir, f"{num_prov}.gpkg")
    engine_url = str(engine.url)

    with engine.begin() as conn:
        rec_t = recintos(conn, ini, fin, num_prov, prov[provincia][1][1])
        lin_t = lineas(conn, ini, fin, num_prov)
        rd_layers = overlay_rd(conn, rec_t, lin_t, num_prov, out_gpkg, engine_url)
        crono_layers = overlay_crono(conn, ini, fin, num_prov, usos_seleccionados, out_gpkg, engine_url)

    print("Proceso completado. GeoPackage generado en", out_gpkg)

if __name__ == '__main__':
    main()
