# import lib.marcadoresupm as upm
from context import lib
from lib.procesamiento.upm import marcadoresupm as upm
import ee
import geemap
import os
import time
import pandas as pd
from tqdm import tqdm
from google.cloud import storage
import google.auth


def opcion1(asset_path, bucket='infolandyn', prefix='Pruebas/FEGA'):
    try:
        ee.Initialize()
    except Exception:
        ee.Authenticate(auth_mode='notebook')
        ee.Initialize()
    if asset_path.endswith('.shp'):
        fc = ee.FeatureCollection(geemap.shp_to_ee(asset_path))
        name = os.path.basename(asset_path).split('.')[0]
    else:
        fc = ee.FeatureCollection(asset_path)
        name = os.path.basename(asset_path)
    # check if province is in the properties
    provincias = fc.aggregate_array('provincia').distinct().getInfo()
    
    for prov in tqdm(provincias, desc='Calculando indices espectrales'):
        tqdm.write(f'Provincia {name}_{prov} en cola')
        fc_prov = fc.filterMetadata('provincia', 'equals', prov)
        fc_prov = upm.calcular_indices_espectrales(fc_prov)
        btch = ee.batch.Export.table.toCloudStorage(collection=fc_prov, description=f'{name}_{prov}', bucket=bucket, fileNamePrefix=f'{prefix}/{name}_{prov}', fileFormat='CSV')
        btch.start()
        while btch.active():
            time.sleep(3)


def opcion2(csv_dir=None):
    if not csv_dir:
        csv_dir = input('Introduce la ruta del directorio con los CSVs: ')
    csvs = os.listdir(csv_dir)
    csvs = [csv for csv in csvs if csv.endswith('.csv') and 'marcadores' not in csv]
    df = pd.DataFrame()
    for csv in tqdm(csvs, desc='Calculando marcadores'):
        df_prov = pd.read_csv(os.path.join(csv_dir, csv))
        # df_prov['provincia'] = csv.split('_')[1].split('.')[0]
        df_prov = upm.calcular_marcadores(df_prov)
        df = pd.concat([df, df_prov])
    if 'ID_PASTOS' not in df.columns:
        # id_pastos concat with '-' provincia, municipio, agregado, zona, poligono, parcela, recinto
        df['ID_PASTOS'] = df.apply(lambda x: f"{x['provincia']}-{x['municipio']}-{x['agregado']}-{x['zona']}-{x['poligono']}-{x['parcela']}-{x['recinto']}", axis=1)
    columns = ['ID_PASTOS', 'TAM', 'INTERVALS', 'ANUALCYCLE', 'TAM_FLAG', 'STABILITY_STATUS', 'NDVI', 'AR', 'AS1']
    df = df[columns]
    df = df.sort_values(by='ID_PASTOS')
    df.to_csv(os.path.join(csv_dir,'marcadores.csv'), index=False)


def opcion3(credentials_path, bucket='infolandyn', prefix='Pruebas/FEGA', output_dir='output'):
    auth = google.auth.load_credentials_from_file(credentials_path)
    storage_client = storage.Client('infolandyn-project', credentials=auth[0])
    bucket = storage_client.get_bucket(bucket)
    blobs = bucket.list_blobs(prefix=prefix)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for blob in tqdm(blobs, desc='Descargando CSVs'):
        if blob.name.endswith('.csv'):
            blob.download_to_filename(os.path.join(output_dir, blob.name.split('/')[-1]))
    print('Descarga completada')
   
   
def opcion4(credentials_path, bucket='infolandyn', prefix='Pruebas/FEGA', output_dir='output'):
    # calcular indices espectrales en Earth Engine
    opcion1('projects/infolandyn-project/assets/lin_abandono_pastos_2023', bucket, prefix)
    # descargar los CSVs
    opcion3(credentials_path, bucket, prefix, output_dir)
    # calcular marcadores
    opcion2(output_dir)
    print('Proceso completado')
    

def opcion5():
    path = input('Introduce la ruta del archivo de parcelas para parametrizar(csv): ')
    dataframe = pd.read_csv(path)
    # parametrizar con el archivo de parcelas
    upm.parametrizar(dataframe)
    print('Parametrización completada')
    
def opcion6(ar_path, as1_path, ndvi_path, dates_path):
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
    if input('¿Desea parametrizar los datos? (s/n): ').lower() == 's':
        upm.parametrizar(df.copy())
        
    df = upm.calcular_marcadores(df)
    df.to_csv(os.path.join(output_path, f'marcadores-{upm.umbral_tam}-{upm.umbral_days}-{upm.umbral_productivity}.csv'), index=False, sep=';')
    print('Marcadores calculados y guardados en:', os.path.join(output_path,  f'marcadores-{upm.umbral_tam}-{upm.umbral_days}-{upm.umbral_productivity}.csv'))
    
        
            
if __name__ == '__main__':
    option = input('Seleccione una opcion de proceso:\n\t[1] Calcular indices espectrales\n\t[2] Calcular marcadores\n\t[3] Descargar CSVs\n\t[4] Completo\n\t[5] Parametrizar'+
                   '\n\t[6] Proceso completo con archivos independientes(AR, AS1, NDVI y Fechas)\nIntroduce el número de la opción: ')
    if option == '1':
        if input('¿Desea autenticarse con Earth Engine? (s/n): ') == 's':
            ee.Authenticate(auth_mode='notebook')
        # opcion1('projects/ee-diegomadrugaramos/assets/FEGA/pastos_provincias')
        opcion1(input('Introduce la ruta del archivo de parcelas: '))
    elif option== '2':
        opcion2()
    elif option== '3':
        opcion3(input('Introduce la ruta del archivo de credenciales: '))
    elif option == '4':
        if input('¿Desea autenticarse con Earth Engine? (s/n): ') == 's':
            ee.Authenticate(auth_mode='notebook')
        opcion4(input('Introduce la ruta del archivo de credenciales: '))
    elif option == '5':
        opcion5()
    elif option == '6':
        opcion6(input('Introduce la ruta del archivo de AR: '),
                input('Introduce la ruta del archivo de AS1: '),
                input('Introduce la ruta del archivo de NDVI: '),
                input('Introduce la ruta del archivo de fechas: '))
    else:
        print('Opción no válida')