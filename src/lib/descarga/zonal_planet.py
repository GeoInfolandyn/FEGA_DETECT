import stackstac
import planetary_computer as pc
import numpy as np
import xarray as xr
from pystac_client import Client
from shapely.geometry import mapping, GeometryCollection
import dask.array as da
import numba as nb
import pandas as pd
from datetime import datetime,timedelta
import time
import os
import rioxarray as rio
import geopandas as gpd
### Omit warnings
import warnings
import sys
import argparse
import subprocess
warnings.filterwarnings('ignore')

orbits = {
    "31SED": 108,
    "31TBE": 51,
    "31TCE": 51,
    "31TCF": 51,
    "31TCG": 51,
    "31TCH": 8,
    "31TCJ": 51,
    "31TDE": 8,
    "31TDF": 8,
    "31TDG": 8,
    "31TDH": 8,
    "31TDJ": 8,
    "31TEE": 108,
    "31TEF": 8,
    "31TEG": 8,
    "31TEH": 8,
    "31TEJ": 8,
    "31TFE": 108,
    "31TFF": 108,
    "31TFG": 108,
    "31TFH": 108,
    "31TFJ": 108,
    "30SXE": 51,
    "30SYE": 8,
    "30SYF": 51,
    "30SYG": 51,
    "30SYH": 51,
    "30SYJ": 51,
    "30TYK": 51,
    "31SBA": 8,
    "31SBB": 8,
    "31SBC": 51,
    "31SBD": 51,
    "31SBV": 8,
    "31SCA": 8,
    "31SCB": 8,
    "31SCC": 8,
    "31SCD": 8,
    "31SCV": 8,
    "31SDA": 108,
    "31SDB": 8,
    "31SDC": 8,
    "31SDD": 8,
    "31SDV": 108,
    "31SEA": 108,
    "31SEB": 108,
    "31SEC": 108,
    "29SLA": 80,
    "29SLB": 80,
    "29SLC": 80,
    "29SLD": 80,
    "29SLV": 80,
    "29SMA": 37,
    "29SMB": 37,
    "29SMC": 37,
    "29SMD": 80,
    "29SMV": 37,
    "29SNC": 37,
    "29SND": 37,
    "29TLE": 80,
    "29TLF": 80,
    "29TLG": 80,
    "29TLH": 123,
    "29TLJ": 123,
    "29TME": 80,
    "29TMF": 80,
    "29TMG": 80,
    "29TMH": 80,
    "29TMJ": 80,
    "29TNE": 37,
    "29TNF": 37,
    "29TNG": 37,
    "29TNH": 80,
    "29TNJ": 80,
    "29TPG": 37,
    "29TPH": 37,
    "29TPJ": 37,
    "29SQA": 137,
    "29SQV": 137,
    "30STE": 137,
    "30STF": 137,
    "30STG": 137,
    "30SUE": 94,
    "30SUF": 94,
    "30SUG": 94,
    "30SUH": 137,
    "30SUJ": 137,
    "30SVE": 94,
    "30SVF": 94,
    "30SVG": 94,
    "30SVH": 94,
    "30SVJ": 94,
    "30SWE": 51,
    "30SWF": 51,
    "30SWG": 94,
    "30SWH": 94,
    "30SWJ": 94,
    "30SXG": 51,
    "30SXH": 51,
    "30SXJ": 51,
    "30TUK": 137,
    "30TUL": 137,
    "30TVK": 94,
    "30TVL": 94,
    "30TVM": 137,
    "30TVN": 137,
    "30TVP": 137,
    "30TWK": 94,
    "30TWL": 94,
    "30TWM": 94,
    "30TWN": 94,
    "30TWP": 94,
    "30TXK": 51,
    "30TXL": 51,
    "30TXM": 94,
    "30TXN": 94,
    "30TXP": 94,
    "30TYL": 51,
    "30TYM": 51,
    "30TYN": 51,
    "30TYP": 51,
    "31TBF": 51,
    "31TBG": 51,
    "31SEV": 108,
    "31SFA": 108,
    "31SFB": 108,
    "31SFC": 108,
    "31SFD": 108,
    "31SFV": 65,
    "29SNA": 37,
    "29SNB": 37,
    "29SNV": 37,
    "29SPA": 137,
    "29SPB": 137,
    "29SPC": 37,
    "29SPD": 37,
    "29SPV": 137,
    "29SQC": 137,
    "29SQD": 137,
    "29TPE": 37,
    "29TPF": 37,
    "29TQE": 137,
    "29TQF": 137,
    "29TQG": 137,
    "29TQH": 37,
    "29TQJ": 37,
    "30STJ": 137,
    "30TTK": 137,
    "30TTL": 137,
    "30TTM": 37,
    "30TUM": 137,
    "30TUN": 137,
    "30TUP": 137,
    "30SXF": 51,
    "29SQB": 137,
    "30STH": 137
}
 

bandas = {
    "B01": "coastal",
    "B02": "blue",
    "B03": "green",
    "B04": "red",
    "B05": "rededge1",
    "B06": "rededge2",
    "B07": "rededge3",
    "B08": "nir",
    "B8A": "rededge4",
    "B09": "water_vapor",
    "B10": "cirrus",
    "B11": "swir1",
    "B12": "swir2",
    "SCL": "scl",
}


cutoff_date = datetime(2022, 1, 24)

def primera_fecha(start_date,end_date,tile,orbit):
    # Asegurarse de que start_date y end_date son objetos datetime
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
    collection = "sentinel-2-l2a"

    # Search for items (tiles) within the AOI and time range using pystac-client
    stac_api_url = "https://planetarycomputer.microsoft.com/api/stac/v1"
    client = Client.open(stac_api_url)
    search = client.search(
        collections=[collection],
        bbox=bbox,
        datetime=start_date.strftime('%Y-%m-%d') + '/' + end_date.strftime('%Y-%m-%d'),
        query={
            "s2:mgrs_tile": {"eq": tile},
            "sat:relative_orbit": {"eq": orbit},
        },
    )
    items = list(search.item_collection())
    items_list = sorted(items, key=lambda x: x.datetime)

    # Get and sign the items
    ini = items_list[0].datetime
    ini = datetime.strptime(str(ini).split(' ')[0], '%Y-%m-%d')
    # print(ini, start_date)
    diff = ini -  start_date 
    ndays = 0
    if diff > timedelta(days=5):
        ndays = diff.days // 5
    ini = ini - timedelta(days=5*ndays) 

    
    all_dates = generate_date_range(ini, end_date, freq='5D')
    missing_dates = [date for date in all_dates if date not in [str(item.datetime).split(' ')[0] for item in items_list]]
    
    return missing_dates, all_dates
    
    

def generate_date_range(start_date, end_date, freq='5D'):
    # Asegurarse de que start_date y end_date son objetos datetime
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Generar el rango de fechas
    all_dates = pd.date_range(start_date, end_date, freq=freq)
    
    # Convertir las fechas a strings en formato 'YYYY-MM-DD'
    all_dates_str = [date.strftime('%Y-%m-%d') for date in all_dates]
    
    return all_dates_str


def calculate_and_save_index(start_date, end_date, tile, orbit, index_name, res=10,outdir= '',formato='ENVI'):
    global df_fin, all_dates
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    if formato == 'GTiff':
        tiff_filename = f"{index_name.upper()}_{tile}_{orbit}_{start_date_str}_{end_date_str}_{res}m.tif"
    elif formato == 'ENVI':
        tiff_filename = f"{index_name.upper()}_{tile}_{orbit}_{start_date_str}_{end_date_str}_{res}m"
    output_filename = os.path.join(outdir, tiff_filename)
    if os.path.exists(output_filename):
        print(f"El archivo {output_filename} ya existe.")
        return
    else:
        print(start_date, end_date, tile, orbit, index_name, res, outdir)

        time_range = f"{start_date_str}/{end_date_str}"
        

        # Sentinel-2 collection on Planetary Computer
        collection = "sentinel-2-l2a"
       
        # Search for items (tiles) within the AOI and time range using pystac-client
        stac_api_url = "https://planetarycomputer.microsoft.com/api/stac/v1"
        client = Client.open(stac_api_url)
        search = client.search(
            collections=[collection],
            bbox=bbox,
            datetime=time_range,
            query={
                "s2:mgrs_tile": {"eq": tile},
                "sat:relative_orbit": {"eq": orbit},
            },
        )
        
        os.makedirs(outdir, exist_ok=True)
        
        # Get and sign the items
        items = list(search.item_collection())
        items_signed = [pc.sign(item) for item in items]
        fechas = set()
        for item in items:
            if item.datetime in fechas:
                items.remove(item)
            else:
                fechas.add(item.datetime)
        
        print(f"Found {len(items)} items for tile {tile} and relative orbit {orbit} from {start_date_str} to {end_date_str}")
        items_list = sorted(items, key=lambda x: x.datetime)

        ### list the dates of the items
        items_dates = [str(item.datetime).split(' ')[0] for item in items_list]
        first_date = items_dates[0]
        print(f"First date: {first_date}")
        print(f"Dates of the items: {items_dates}")

        ### compare the dates of the items with the dates of the all_dates list and list the missing dates

        missing_dates = [date for date in all_dates if date not in items_dates and (datetime.strptime(date, '%Y-%m-%d') >= start_date and datetime.strptime(date, '%Y-%m-%d') <= end_date)]
        print(f"Missing dates: {missing_dates}")
        

        df = pd.DataFrame({"fecha": [str(all_dates) for all_dates in all_dates]}).sort_values(by="fecha", key=lambda x: pd.to_datetime(x, format='%Y-%m-%d', errors='coerce'), ignore_index=True)
        
        df_fin = pd.concat([df_fin,df], axis=0)

        # Load the required bands for index calculations using dask
        
        nir_band = "B08" if res == 10 else "B8A"
        required_assets = {
        "ndvi": ["B04", nir_band,"SCL"],
        "gndvi": ["B03", nir_band,"SCL"],
        "lai": ["B04", nir_band,"SCL"],
        "evi": ["B04", nir_band,"B02","SCL"],
        "lai_evi": ["B04", nir_band,"B02","SCL"],
        "sr": ["B04", nir_band,"SCL"],
        "nir": [nir_band,"SCL"],
        "red": ["B04","SCL"],
        "saavi": ["B04", nir_band,"SCL"],
        "fapar": ["B03", nir_band,"SCL"],
        "ar": ["B03", "B04", nir_band,"SCL"],
        "as1": [nir_band, "B11", "B12","SCL"],
        "sasi": [nir_band, "B11", "B12","SCL"],
        "anir": ["B04", nir_band, "B11","SCL"],
        "are1": ["B04", "B05", "B06","SCL"],
        "are2": ["B05", "B06", "B07","SCL"],
        "are3": ["B06", "B07", nir_band,"SCL"],
        "nbr": [nir_band, "B12","SCL"],
        "conc": ["B08", "B11", "B12","SCL"],
        "B1": ["B01","SCL"],
        "B2": ["B02","SCL"],
        "B3": ["B03","SCL"],
        "B4": ["B04","SCL"],
        "B5": ["B05","SCL"],
        "B6": ["B06","SCL"],
        "B7": ["B07","SCL"],
        "B8": [nir_band,"SCL"],
        "B9": ["B09","SCL"],
        "B10": ["B10","SCL"],
        "B11": ["B11","SCL"],
        "B12": ["B12","SCL"],
        "SCL": ["SCL"]

        }
        
        ### Make a dicctionary with the required assets for each index and the quality band
        os.environ["GDAL_HTTP_MAX_RETRY"] = "10"
        os.environ["GDAL_HTTP_RETRY_DELAY"] = "5"
        stack = stackstac.stack(
            items_signed,
            assets=required_assets[index_name.lower()],
            resolution=res,  # 10-meter resolution
            epsg=32630,  # UTM zone (adjust according to your location)
            gdal_env=stackstac.DEFAULT_GDAL_ENV.updated(always=dict(GDAL_HTTP_MAX_RETRY=10, GDAL_HTTP_RETRY_DELAY=5	))
        )
        stack = stack.chunk({"x": -1, "y": -1})  # Chunk the data for parallel processing

        ### ADD THE MISSING DATES TO THE dask array as a 0 image
        missing_dates = [datetime.strptime(date, '%Y-%m-%d') for date in missing_dates]
        print("Stack shape:", stack.shape)
        
        _, unique_indices = np.unique(stack['time'], return_index=True)
        stack = stack.isel(time=unique_indices)

        zero_data = xr.DataArray(
        np.zeros((len(missing_dates), len(stack.y), len(stack.x), len(stack.band))),
            coords= {
                "time": missing_dates,
                "y": stack.y.data,
                "x": stack.x.data,
                "band": stack.band
            },        
            dims=['time', 'y', 'x', 'band']
        )
        zero_data = zero_data.transpose('time', 'band', 'y', 'x')
        print("Zero data shape after transpose:", zero_data.shape)

        # Get all unique times
        all_times = sorted(set(stack.time.values) | set(zero_data.time.values))

        # Create a new DataArray with all times
        combined = xr.DataArray(
            np.full((len(all_times), len(stack.band), len(stack.y), len(stack.x)), np.nan),
            coords={
                'time': all_times,
                'band': stack.band.values,
                'y': stack.y.values,
                'x': stack.x.values
            },
            dims=['time', 'band', 'y', 'x']
        )
        
        combined = combined.rio.write_crs("EPSG:32630")
        
        # Fill in data from stack
        combined.loc[dict(time=stack.time.values)] = stack.values
        
        # Fill in data from zero_data
        combined.loc[dict(time=zero_data.time.values)] = zero_data.values      
        
        print("Combined shape:", combined.shape)
        
        stack = combined.chunk({"x": -1, "y": -1})  # Chunk the data for parallel processing
        
        # Cloud masking
        cloud_mask = (stack.sel(band="SCL") != 8) & (stack.sel(band="SCL") != 9) & (stack.sel(band="SCL") != 10) & (stack.sel(band="SCL") != 11) & (stack.sel(band="SCL") != 3) & (stack.sel(band="SCL") != 2) & (stack.sel(band="SCL") != 1) & (stack.sel(band="SCL") != 0)        
        
        stack = stack.where(cloud_mask)
        stack.rio.write_crs("EPSG:32630")

        # Define index calculation functions
        @nb.jit(nopython=True)
        def calc_ndvi(b4, b8):
            ndvi = ((b8 - b4) / (b8 + b4)) * 10000
            return ndvi.astype(np.int16)

        @nb.jit(nopython=True)
        def calc_gndvi(b3, b8):
            gndvi = ((b8 - b3) / (b8 + b3)) * 10000
            return gndvi.astype(np.int16)

        @nb.jit(nopython=True)
        def calc_lai(b4, b8):
            lai = (0.57 * np.exp(2.33 * ((b8 - b4) / (b8 + b4)))) * 1000
            return lai.astype(np.uint16)

        @nb.jit(nopython=True)
        def calc_evi(b4, b8, b2):
            evi = (2.5 * ((b8 - b4) / (b8 + 6 * b4 - 7.5 * b2 + 1)))*1000
            return evi.astype(np.int16)

        @nb.jit(nopython=True)
        def calc_lai_evi(b4, b8, b2):
            lai_evi = (3.618 * ((b8 - b4) / (b8 + 6 * b4 - 7.5 * b2 + 1)) - 0.118)*1000
            return lai_evi.astype(np.uint16)

        @nb.jit(nopython=True)
        def calc_sr(b4, b8):
            sr = b8 / b4 * 10000
            return sr.astype(np.int16)
        @nb.jit(nopython=True)
        def calc_nir(b8):
            return b8
        
        @nb.jit(nopython=True)
        def calc_red(b4):
            return b4
        
        @nb.jit(nopython=True)
        def calc_saavi(b4, b8):
            saavi = ((b8 - b4) / (b8 + b4 + 0.5)) * 1000
            return saavi.astype(np.int16)
        
        @nb.jit(nopython=True)
        def calc_fapar(b3, b8):
            gndvi = ((b8 - b3) / (b8 + b3))
            fapar = (0.026 * np.exp(4.083*gndvi)) * 100
            return fapar.astype(np.int16)
        
        @nb.jit(nopython=True)
        def calc_ar(b3, b4, b8):
            a_ar = np.sqrt((664-560)**2 + (b4 - b3)**2)
            b_ar = np.sqrt((835-664)**2 + (b8 - b4)**2)
            c_ar = np.sqrt((835-560)**2 + (b8 - b3)**2)
            
            cos_ar = (a_ar**2 + b_ar**2 - c_ar**2) / (2 * a_ar * b_ar)
            # Clip values to valid cosine range [-1, 1]
            cos_ar = np.minimum(np.maximum(cos_ar, -1.0), 1.0)
            # Calculate arccos and convert to degrees
            ar = np.arccos(cos_ar) * 10000  
            return ar.astype(np.uint16)
        
        @nb.jit(nopython=True)
        def calc_as1(b8,b11,b12):
            a_as1 = np.sqrt((1613.7-835)**2 + (b11 - b8)**2)
            b_as1 = np.sqrt((1613.7-2202)**2 + (b11 - b12)**2)
            c_as1 = np.sqrt((2202-835)**2 + (b12 - b8)**2)
            cos_as1 = (a_as1**2 + b_as1**2 - c_as1**2) / (2 * a_as1 * b_as1)
            cos_as1 =  np.minimum(np.maximum(cos_as1, -1.0), 1.0)
            as1 = np.arccos(cos_as1)*10000
            return as1.astype(np.int16)
        
        @nb.jit(nopython=True)
        def calc_sasi(b8,b11,b12):
            beta_sasi = calc_as1(b8,b11,b12)
            pend = b11 - b8
            sasi = beta_sasi / pend
            sasi = sasi*10000

            return sasi.astype(np.uint32)
        
        @nb.jit(nopython=True)
        def calc_anir(b4,b8,b11):
            a_anir = np.sqrt((835-664)**2 + (b8 - b4)**2)
            
            b_anir = np.sqrt((1613.7-835)**2 + (b11 - b8)**2)
            
            c_anir = np.sqrt((1613.7-664)**2 + (b11 - b4)**2)
            
            cos_anir = (a_anir**2 + b_anir**2 - c_anir**2) / (2 * a_anir * b_anir)
            
            cos_anir = np.minimum(np.maximum(cos_anir, -1.0), 1.0)
            
            anir = np.arccos(cos_anir)*10000

            return anir.astype(np.uint16)
        
        @nb.jit(nopython=True)
        def calc_are1(b4,b5,b6):
            a_are1 = np.sqrt((705-665)**2 + (b5 - b4)**2)
            b_are1 = np.sqrt((740-705)**2 + (b6 - b5)**2)
            c_are1 = np.sqrt((740-665)**2 + (b6 - b4)**2)
            
            cos_are1 = (a_are1**2 + b_are1**2 - c_are1**2) / (2 * a_are1 * b_are1)
            cos_are1 = np.minimum(np.maximum(cos_are1, -1.0), 1.0)
            are1 = np.arccos(cos_are1)*10000
            return are1.astype(np.uint16)
        @nb.jit(nopython=True)
        def calc_are2(b5,b6,b7):
            a_are2 = np.sqrt((740-705)**2 + (b6 - b5)**2)
            b_are2 = np.sqrt((783-740)**2 + (b7 - b6)**2)
            c_are2 = np.sqrt((783-705)**2 + (b7 - b5)**2)
            
            cos_are2 = (a_are2**2 + b_are2**2 - c_are2**2) / (2 * a_are2 * b_are2)
            cos_are2 = np.minimum(np.maximum(cos_are2, -1.0), 1.0)
            are2 = np.arccos(cos_are2)*10000
            return are2.astype(np.uint16)
        
        @nb.jit(nopython=True)
        def calc_are3(b6,b7,b8):
            a_are3 = np.sqrt((783-740)**2 + (b7 - b6)**2)
            b_are3 = np.sqrt((842-783)**2 + (b8 - b7)**2)
            c_are3 = np.sqrt((842-740)**2 + (b8 - b6)**2)
            
            cos_are3 = (a_are3**2 + b_are3**2 - c_are3**2) / (2 * a_are3 * b_are3)
            cos_are3 = np.minimum(np.maximum(cos_are3, -1.0), 1.0)
            are3 = np.arccos(cos_are3)*10000
            return are3.astype(np.uint16)
        
        @nb.jit(nopython=True)
        def calc_nbr(b8,b12):
            nbr = (b8 - b12) / (b8 + b12) *10000
            return nbr.astype(np.int16)
        
        @nb.jit(nopython=True)
        def calc_conc(b8, b11, b12):
            wl_b9 = 842
            wl_b11 = 1610
            wl_b12 = 2190
            interp = (b8 - b11) / (b8 + b11) * (wl_b11 - wl_b9) + (b8 - b12) / (b8 + b12) * (wl_b12 - wl_b9)
            conc = b11 - interp
            return conc.astype(np.int16)
            
        
        @nb.jit(nopython=True)
        def calc_b1(b1):
            return b1
        
        @nb.jit(nopython=True)
        def calc_b2(b2):
            return b2
        @nb.jit(nopython=True)
        def calc_b3(b3):
            return b3
        @nb.jit(nopython=True)
        def calc_b4(b4):
            return b4
        @nb.jit(nopython=True)
        def calc_b5(b5):
            return b5
        @nb.jit(nopython=True)
        def calc_b6(b6):
            return b6
        @nb.jit(nopython=True)
        def calc_b7(b7):
            return b7
        @nb.jit(nopython=True)
        def calc_b8(b8):
            return b8
        @nb.jit(nopython=True)
        def calc_b9(b9):
            return b9
        @nb.jit(nopython=True)
        def calc_b10(b10):
            return b10
        @nb.jit(nopython=True)
        def calc_b11(b11):
            return b11
        @nb.jit(nopython=True)
        def calc_b12(b12):
            return b12
        @nb.jit(nopython=True)
        def calc_scl(scl):
            return scl

        bandas = {}
        for band in stack.band.values:

            if band == "SCL":
                continue        
            banda = stack.sel(band=band).data

            if start_date >= cutoff_date: 
                bam = stack.sel(band=band).data
                banda = da.map_blocks(lambda x: x - 1000, bam, dtype=bam.dtype)
                
            bandas[band] = da.where(banda < 0, 0, banda)

        # Calculate the selected index
        if index_name.lower() == "ndvi":
            index_data = da.map_blocks(calc_ndvi, bandas["B04"], bandas[nir_band], dtype=np.int16)
        elif index_name.lower() == "gndvi":
            index_data = da.map_blocks(calc_gndvi, bandas["B03"],  bandas[nir_band], dtype=np.int16)
        elif index_name.lower() == "lai":
            index_data = da.map_blocks(calc_lai, bandas["B04"],  bandas[nir_band], dtype=np.uint16)
        elif index_name.lower() == "evi":
            index_data = da.map_blocks(calc_evi, bandas["B04"],  bandas[nir_band], bandas["B02"], dtype=np.int16)
        elif index_name.lower() == "lai_evi":
            index_data = da.map_blocks(calc_lai_evi, bandas["B04"],  bandas[nir_band], bandas["B02"], dtype=np.int16)
        elif index_name.lower() == "sr":
            index_data = da.map_blocks(calc_sr, bandas["B04"],  bandas[nir_band], dtype=np.int16)
        elif index_name.lower() == "nir":
            index_data = da.map_blocks(calc_nir, bandas[nir_band])
        elif index_name.lower() == "red":
            index_data = da.map_blocks(calc_red, bandas["B04"])    
        elif index_name.lower() == "saavi":
            index_data = da.map_blocks(calc_saavi, bandas["B04"],  bandas[nir_band], dtype=np.int16)
        elif index_name.lower() == "fapar":
            index_data = da.map_blocks(calc_fapar, bandas["B03"],  bandas[nir_band], dtype=np.int16)
        elif index_name.lower() == "ar":
            index_data = da.map_blocks(calc_ar, bandas["B03"],  bandas["B04"], bandas[nir_band], dtype=np.uint16)
        elif index_name.lower() == "as1":
            index_data = da.map_blocks(calc_as1, bandas[nir_band],  bandas["B11"], bandas["B12"], dtype=np.int16)
        elif index_name.lower() == "sasi":
            index_data = da.map_blocks(calc_sasi, bandas[nir_band],  bandas["B11"], bandas["B12"], dtype=np.uint32)
        elif index_name.lower() == "anir":
            index_data = da.map_blocks(calc_anir, bandas["B04"],  bandas[nir_band], bandas["B11"], dtype=np.uint16)
        elif index_name.lower() == "are1":
            index_data = da.map_blocks(calc_are1, bandas["B04"],  bandas["B05"], bandas["B06"], dtype=np.uint16)
        elif index_name.lower() == "are2":
            index_data = da.map_blocks(calc_are2, bandas["B05"],  bandas["B06"], bandas["B07"], dtype=np.uint16)
        elif index_name.lower() == "are3":
            index_data = da.map_blocks(calc_are3, bandas["B06"],  bandas["B07"], bandas[nir_band], dtype=np.uint16)
        elif index_name.lower() == "nbr":
            index_data = da.map_blocks(calc_nbr, bandas[nir_band],  bandas["B12"], dtype=np.int16)
        elif index_name.lower() == "b1":
            index_data = da.map_blocks(calc_b1, bandas["B01"])
        elif index_name.lower() == "b2":
            index_data = da.map_blocks(calc_b2, bandas["B02"])
        elif index_name.lower() == "b3":
            index_data = da.map_blocks(calc_b3, bandas["B03"])
        elif index_name.lower() == "b4":
            index_data = da.map_blocks(calc_b4, bandas["B04"])
        elif index_name.lower() == "b5":
            index_data = da.map_blocks(calc_b5, bandas["B05"])
        elif index_name.lower() == "b6":
            index_data = da.map_blocks(calc_b6, bandas["B06"])
        elif index_name.lower() == "b7":
            index_data = da.map_blocks(calc_b7, bandas["B07"])
        elif index_name.lower() == "b8":
            index_data = da.map_blocks(calc_b8, bandas[nir_band])
        elif index_name.lower() == "b9":
            index_data = da.map_blocks(calc_b9, bandas["B09"])
        elif index_name.lower() == "b10":
            index_data = da.map_blocks(calc_b10, bandas["B10"])
        elif index_name.lower() == "b11":
            index_data = da.map_blocks(calc_b11, bandas["B11"])
        elif index_name.lower() == "b12":
            index_data = da.map_blocks(calc_b12, bandas["B12"])
        elif index_name.lower() == "scl":
            index_data = da.map_blocks(calc_scl, bandas["SCL"])
        elif index_name.lower() == "conc":
            index_data = da.map_blocks(calc_conc, bandas["B08"], bandas["B11"], bandas["B12"], dtype=np.uint8)
        

        else:
            print(f"Error: Índice '{index_name}' no reconocido.")
            return

        # Convert the Dask array to an xarray.DataArray and rename the spatial dimension
        index_data = xr.DataArray(index_data, coords= {'time': stack.time,
                                                    'y': stack.y, 
                                                    'x': stack.x}, dims=['time', 'y', 'x'])
        index_data = index_data.where(index_data >= -1000, 0)
        index_data = index_data.rio.write_crs(stack.rio.crs)

        # Save the index as a Envi

        index_data.rio.to_raster(output_filename, driver= formato, dtype=np.int16, crs = stack.rio.crs, transform = stack.rio.transform())
       
        print(f"{index_name} calculated and saved as a 16-bit integer GeoTIFF: {output_filename}")

        
def calculate_and_save_index_annually(start_date, end_date, tile, orbit, index_name, res=10, outdir='', formato='ENVI'):

    current_date = start_date
    
    # Sentinel-2 collection on Planetary Computer
    collection = "sentinel-2-l2a"
    flag = start_date > cutoff_date  # Set flag to True if start_date is after cutoff_date

    while current_date < end_date:
        time_delta = timedelta(days=40) if res == 10 else timedelta(days=180) if res == 20 else timedelta(days=730)
    
        if flag:
        # If we're already past the cutoff date, just use the normal time delta
            current_end_date = current_date + time_delta
        else:
            # If we haven't reached the cutoff date yet, check if this interval crosses it
            if current_date + time_delta > cutoff_date:
                current_end_date = cutoff_date
                flag = True
            else:
                current_end_date = current_date + time_delta

        # Ensure we don't go past the overall end date
        if current_end_date > end_date:
            current_end_date = end_date

        # Ensure that current_end_date is always after current_date
        if current_end_date <= current_date:
            current_date = current_end_date + timedelta(days=1)
            continue

        time_range = f"{current_date.strftime('%Y-%m-%dT%H:%M:%SZ')}/{current_end_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        print(f"Searching from {current_date} to {current_end_date}")


        # Pass start and end dates to the index calculation function
        ahora = time.time()
        calculate_and_save_index(current_date, current_end_date, tile, orbit, index_name, res, outdir, formato)
        print(current_date,current_end_date)
        print('-'*50+ 'Calculado' + '-'*50)
        
        tiempo_horas = (time.time() - ahora) // 3600
        tiempo_minutos = (time.time() - ahora) / 60 % 60
        tiempo_segundos = (time.time() - ahora) % 60

        print(f"Tiempo de ejecución: {int(tiempo_horas)}:{int(tiempo_minutos)}:{int(tiempo_segundos)} horas ")
        print('-'*100)
        current_date = current_end_date + timedelta(days=1)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Calculate a vegetation index from Sentinel-2 data")
    parser.add_argument("outdir", help="Output directory")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("index_name", help="Name of the index to calculate")
    parser.add_argument("res", type=int, help="Resolution of the output raster in meters")
    parser.add_argument("driver", choices=["ENVI","NetCDF"],default="ENVI", help="Output raster driver")
    parser.add_argument("--tile", help="MGRS tile if not using a shapefile")
    parser.add_argument("--clip_path", help="Path to a shapefile to clip the output raster")

    return parser.parse_args()
def main():
    global df_fin, all_dates, bbox
    ahora = time.time()
    args = parse_args()
    outdir = args.outdir
    # Define the time range
    year, month, day = map(int, args.start_date.split('-'))
    start_date = datetime(year, month, day)
    
    year, month, day = map(int, args.end_date.split('-'))
    end_date = datetime(year, month, day)
    res = args.res
    formato = args.driver
    index_name = args.index_name

    print(args)
    # Define the tile and relative orbit
    
    
    if args.clip_path:
        clip_path = args.clip_path
        tiles = gpd.read_file(r"data/TILES/TILES.shp").to_crs("EPSG:4326")

        roi = gpd.read_file(clip_path).to_crs("EPSG:4326")
        if 'Name' in roi.columns:
            roi = roi.rename(columns={'Name': 'Name_roi'})
        bbox = roi.total_bounds
        tile_inter = gpd.overlay(roi, tiles, how='intersection')
        ### select the tile polygon with the largest area and take the name of the tile
        tile = tile_inter[tile_inter.area == tile_inter.area.max()]["Name"].values[0]
        orbit = orbits[tile]
        print(f"Tile: {tile}, Orbit: {orbit}")

    else:
        tile = args.tile
        orbit = orbits[tile]
        bbox = None
        clip_path = None       

    outdir = os.path.join(outdir, f"{tile}_{index_name}_{res}m")

    # Ensure the output directory exists
    os.makedirs(outdir, exist_ok=True)
    _, all_dates = primera_fecha(start_date, end_date, tile, orbit)
    # Calculate and save the index
    df_fin = pd.DataFrame()
    calculate_and_save_index_annually(start_date, end_date, tile, orbit, index_name, res, outdir, formato)
    ### run the stack function with a subprocess using python 3.6
    

    # Save the CSV file
    csv_path = os.path.join(outdir, 'fechas.csv')
    df_fin.to_csv(csv_path, index=False)
    print(f"CSV file saved to: {csv_path}")

    
    print("Proceso finalizado.")
    tiempo_horas = (time.time() - ahora) // 3600
    tiempo_minutos = (time.time() - ahora) / 60 % 60
    tiempo_segundos = (time.time() - ahora) % 60
    print(f"Tiempo de ejecución: {int(tiempo_horas)}:{int(tiempo_minutos)}:{int(tiempo_segundos)} horas ")
    # subprocess.run(["python3.6", r"lib\descarga\stack_stacks.py", outdir, f"stack_{tile}_{index_name}_{res}.dat"])
    # subprocess.run(["python3.6", r"/scripts/pruebas_optimizados/interpolacion.py", os.path.join(outdir, f"stack_{tile}_{index_name}_{res}.dat"),"linear"])

if __name__ == "__main__":
    main()