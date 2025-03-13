import ee
import pandas as pd
import datetime

## Constants
umbral_tam = 0.4
umbral_productivity = 10000
umbral_days = 60

## Functions
def calcular_indices_espectrales(asset) -> ee.FeatureCollection:
    """Calculates de spectral indexes (NDVI, AS1, and AR) of a plot with Sentinel-2 images in Google Earth Engine.

    Args:
        asset (ee.FeatureCollection): FeatureCollection with the plots to calculate the indexes

    Returns:
        ee.FeatureCollection: FeatureCollection with the indexes calculated
    """
    def ndvi_ar_as1(img):
        """Map function to calculate the NDVI, AR, and AS1 indexes of a Sentinel-2 image in Google Earth Engine.
        \nThe indexes are returned as integer values multiplied by 10000.

        Args:
            img (ee.Image): Sentinel-2 image

        Returns:
            ee.Image: Sentinel-2 image with the NDVI, AR, and AS1 indexes calculated
        """
        # NDVI
        ndvi = img.normalizedDifference(['B8', 'B4']).multiply(10000).toInt16().rename('NDVI')
        img = img.addBands(ndvi)
        
        # AR
        a_ar = img.expression('(sqrt((664.7-560)**2 + (B4-B3)**2))',
                        {
            'B4': img.select('B4'),
            'B3': img.select('B3')
        }).rename('a_Ar')
        b_ar = img.expression('(sqrt((835-664.7)**2 + (B8-B4)**2))',
                            {
                    'B8': img.select('B8'),
                    'B4': img.select('B4')
                }).rename('b_Ar')
        c_ar = img.expression('(sqrt((835-560)**2 + (B8-B3)**2))',
                            {
                    'B8': img.select('B8'),
                    'B3': img.select('B3')
                }).rename('c_Ar')
        ar = img.expression('(a_Ar**2 + b_Ar**2 - c_Ar**2) / (2 * a_Ar * b_Ar)',
                            {
                'a_Ar': a_ar,
                'b_Ar': b_ar,
                'c_Ar': c_ar
            }).clamp(-1,1).acos().multiply(10000).toUint16().rename('AR')
        img = img.addBands(ar)

        # AS1
        a_as1 = img.expression('(sqrt((1613.7-835)**2 + (B11-B8)**2))',
                            {
                    'B11': img.select('B11'),
                    'B8': img.select('B8')
                }).rename('a_AS1')
        b_as1 = img.expression('(sqrt((1613.7-2202)**2 + (B11-B12)**2))',
                            {
                    'B11': img.select('B11'),
                    'B12': img.select('B12')
                }).rename('b_AS1')
        c_as1 = img.expression('(sqrt((2202-835)**2 + (B12-B8)**2))',
                            {
                    'B12': img.select('B12'),
                    'B8': img.select('B8')
                }).rename('c_AS1')
        as1 = img.expression('(a_AS1**2 + b_AS1**2 - c_AS1**2) / (2 * a_AS1 * b_AS1)',
                            {
                'a_AS1': a_as1,
                'b_AS1': b_as1,
                'c_AS1': c_as1
            }).clamp(-1,1).acos().multiply(10000).toUint16().rename('AS1')
        img = img.addBands(as1)
        
        # Máscara de calidad
        quality = img.select('SCL')
        mask = quality.eq(8).Or(quality.eq(9)).Or(quality.eq(10)).Or(quality.eq(11)).Or(quality.eq(3)).Or(quality.eq(2)).Or(quality.eq(1)).Or(quality.eq(0))
        condition = mask.eq(0)
        condition = condition.Not()
        img = img.where(condition, 0)
        
        return img  

    def processFeature(feature):
        def convert_value(k, v):
            v = ee.Algorithms.If(ee.Algorithms.IsEqual(v, None), 0, v)  # Si v es None, poner 0
            
            contains_AS1 = ee.String(k).index('AS1').neq(-1)
            contains_AR = ee.String(k).index('AR').neq(-1)
            condition = contains_AS1.Or(contains_AR)

            return ee.Algorithms.If(condition, ee.Number(v).toUint16(), ee.Number(v).toInt16())

        feature = ee.Feature(feature)
        # img_coll = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterDate('2020-01-01', datetime.date.today().strftime('%Y-%m-%d')).filterBounds(feature.geometry())
        img_coll = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterDate('2020-01-01', f'{datetime.date.today().year-1}-12-31').filterBounds(feature.geometry())
        img_coll = img_coll.filterMetadata('MGRS_TILE', 'equals', img_coll.aggregate_array('MGRS_TILE').distinct().get(0)).filterMetadata('SENSING_ORBIT_NUMBER', 'equals', img_coll.aggregate_array('SENSING_ORBIT_NUMBER').distinct().get(0))
        # img_coll = img_coll.map(lambda img: img.clip(feature.geometry()))
        indexes = img_coll.map(ndvi_ar_as1).select(['NDVI', 'AR', 'AS1'])
        # zonal stats of the image collection for the feature, generating a mean time series of the indexes for the feature
        reduced_ts = indexes.toBands().reduceRegion(reducer=ee.Reducer.mean(), geometry=feature.geometry(), scale=10, maxPixels=1e13)
        # if the k contains AS1 or AR uint16, else (NDVI) int16
        reduced_ts = reduced_ts.map(convert_value)
        return feature.set(reduced_ts)
    
    return asset.map(processFeature)


def __temporal_angle(array1, array2):
    """Calculate the temporal angle mapper between two arrays
    
    Args:
        array1: first array
        array2: second array
    
    Returns:
        np.array with the spectral angle mapper between the two arrays
    """
    import numpy as np
    mod_1 = np.nansum(array1**2)**0.5
    mod_2 = np.nansum(array2**2)**0.5
    dot_product = np.nansum(array1*array2)
    cos_theta = dot_product/(mod_1*mod_2) if mod_1*mod_2 != 0 else 0
    return float(np.arccos(cos_theta))


def __date_intervals(series, dates):
    """Calculate the intervals of the series with values greater than 0

    Args:
        series (np.array): Array with the values of the serie
        dates (list): List with the dates of the serie  

    Returns:
        tuple: tuple with the intervals and the days of each interval
    """
    import datetime
    import numpy as np
        
    indexes = np.nonzero(series)[0]
    nans = np.isnan(series)
    indexes = indexes[~nans[indexes]]
    intervals = []
    amplitude = []
    if len(indexes) == 0:
        return []
    start = indexes[0]
    end = indexes[0]
    for i in range(1, len(indexes)-1):
        if indexes[i+1] - indexes[i] == 1:
            end = indexes[i+1]
        else:
            intervals.append((dates[start], dates[end]))
            amplitude.append(float(np.nanmax(series[start:end+1])))
            start = indexes[i+1]
            end = indexes[i+1]
    intervals.append((dates[start], dates[end]))
    amplitude.append(float(np.nanmax(series[start:end+1])))
    days = [(datetime.datetime.strptime(date[1], '%d/%m/%Y') - datetime.datetime.strptime(date[0], '%d/%m/%Y')).days for date in intervals]
    return list(zip(intervals, days, amplitude))


def __interanualTAM(serie, alldates) -> list:
    """Calculate the interannual spectral angle mapper for each row
    
    Args:
        serie: np.array with the series to compare
    Returns:
        dict with the interannual spectral angle mapper for each row
    """
    import numpy as np
    res = []
    # n_years = serie.shape[0]//73
    n_years = alldates[-1].year - alldates[0].year 
    # repeat the last value to complete the last year
    while serie.shape[0] % 73 != 0:
        serie = np.append(serie, serie[-1])
    series_years = np.array_split(serie, n_years+1) # +1 because the last year is included
    # for i in range(n_years-1):
    #     res[row['ID_PASTOS']].append(spectral_angle(series_years[i], series_years[i+1]))
    for i in range(n_years+1):
        for j in range(i+1, n_years+1):
            res.append(__temporal_angle(series_years[i], series_years[j]))
    return res


def __process_intervals(intervals, ini, end):
    """Calculate the anual cycle of the intervals, making a string with 0 is the year is not present and 1 if it is present

    Args:
        intervals (list): List with the intervals

    Returns:
        Tuple[str, List[str]]: tuple with the anual cycle and the years of the intervals
    """
    res = 'c'
    years = [str(year) for year in range(ini, end+1)]
    for year in years:
        if year == years[0]:
            if any([interval for interval in intervals if year in interval[0][0] and interval[1] > umbral_days and interval[1] < 365 and interval[2] > umbral_productivity]):
                res += '1'
            else:
                res += '0'
        else:
            if any([interval for interval in intervals if year in interval[0][1] and interval[1] > umbral_days and interval[1] < 365 and interval[2] > umbral_productivity]):
                res += '1'
            else:
                res += '0'
                
    return res


def __intervals(ar, as1, alldates):
    """Calculate the intervals of the series with values greater than 0
    
    Args:
        ar: np.array with the AR values
        as1: np.array with the AS1 values
        alldates: list with all the dates from the first date to the last date
    
    Returns:
        list with the intervals of the series
    """    
    import numpy as np
    diff = as1 - ar
    diff = np.where(diff < 0, 0, diff)
    # create a list with the dates starting from 02/01/2020
    dates = [date.strftime('%d/%m/%Y') for date in alldates]
    intervals = __date_intervals(diff, dates)
    return intervals


def __anualcycle(as1, ar, alldates):
    intervals = __intervals(ar, as1, alldates)
    
    return __process_intervals(intervals, alldates[0].year, alldates[-1].year)


def process_series(dataframe:pd.DataFrame) -> pd.DataFrame:
    """

    Args:
        dataframe (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    import numpy as np
    
    ndvi_colums = [col for col in dataframe.columns if 'NDVI' in col]
    ar_colums = [col for col in dataframe.columns if 'AR' in col]
    as1_colums = [col for col in dataframe.columns if 'AS1' in col]
    dates_sentinel2 = list(map(lambda x: x.split('T')[0], ndvi_colums))
    # transform the dates to datetime objects
    dates_sentinel2 = [datetime.datetime.strptime(date, '%Y%m%d') for date in dates_sentinel2]
    # calculate the missing dates from the first date to the last date with a difference of 5 days
    all_dates = [dates_sentinel2[0] + datetime.timedelta(days=i*5) for i in range((dates_sentinel2[-1] - dates_sentinel2[0]).days//5)]
    # check if the dates are 5 days apart
    missing_dates = [date for date in all_dates if date not in dates_sentinel2]
    
    def __filter_serie(serie, all_dates, missing_dates):
        from scipy.signal import savgol_filter
        from scipy.interpolate import interp1d
        import numpy as np
        serie_complete = []
        for date in all_dates:
            if date in missing_dates:
                serie_complete.append(0)
            else:
                serie_complete.append(serie[dates_sentinel2.index(date)])
        serie = np.array(serie_complete)   
        goods = np.nonzero(serie)[0]
        inds = np.arange(serie.shape[0])
        f = interp1d(inds[goods], serie[goods], kind='linear', fill_value="extrapolate")
        serie_int = np.where(serie == 0, f(inds), serie)
        serie_int = savgol_filter(serie_int, 5, 2)
        return serie_int
    
    dataframe['NDVI'] = dataframe[ndvi_colums].apply(lambda x: __filter_serie(np.array(x), all_dates, missing_dates), axis=1)
    dataframe['AR'] = dataframe[ar_colums].apply(lambda x: __filter_serie(np.where(np.array(x) < 7000, 0, np.array(x)), all_dates, missing_dates), axis=1)
    dataframe['AS1'] = dataframe[as1_colums].apply(lambda x: __filter_serie(np.where(np.array(x) < 7000, 0, np.array(x)), all_dates, missing_dates), axis=1)
    dataframe.drop(columns=ndvi_colums+ar_colums+as1_colums, inplace=True)
    
    return dataframe, all_dates


def calcular_marcadores(dataframe:pd.DataFrame) -> pd.DataFrame:
    """Calculates the markers of the dataframe with the spectral indexes

    Args:
        dataframe(pd.DataFrame): DataFrame with the spectral indexes

    Returns:
        pd.DataFrame: DataFrame with the markers calculated
    """
    import numpy as np
    
    def load_umbrals():
        global umbral_tam, umbral_productivity, umbral_days
        try:
            umbrals = pd.read_csv('umbrals/umbrals.csv')
            umbral_tam = umbrals['UMBRAL_TAM'][0]
            umbral_days = umbrals['UMBRAL_DAYS'][0]
            umbral_productivity = umbrals['UMBRAL_PRODUCTIVITY'][0]
        except Exception:
            print('No se han encontrado los umbrals, se usarán los valores por defecto:\nUMBRAL TAM: 0.4\nUMBRAL DAYS: 60\nUMBRAL PRODUCTIVITY: 10000')
            umbral_tam = 0.4
            umbral_productivity = 10000
            umbral_days = 60
            
    def get_TAM_flag(tam):
        if len(tam) == 0:
            flag = -1
        else:
            flag = 1 if any([s > umbral_tam for s in tam]) else 0
        return flag
    
    def classificar(cycle, sam_flag):        
        count_0 = cycle.count('0')
        if sam_flag == -1 or (count_0 == len(cycle)-1) or (count_0 >= (len(cycle)-1)//2 and sam_flag == 1):
            return 'ROJO'
        elif count_0 > 0 or cycle.endswith('0'):
            return 'NARANJA'
        return 'VERDE'

    dataframe, all_dates = process_series(dataframe)

    dataframe['TAM'] = dataframe['NDVI'].apply(lambda x: __interanualTAM(np.array(x), all_dates))
    dataframe['INTERVALS'] = dataframe.apply(lambda x: __intervals(np.array(x['AS1']), np.array(x['AR']), all_dates), axis=1)
    
    load_umbrals()
    
    dataframe['ANUALCYCLE'] = dataframe.apply(lambda x: __anualcycle(np.array(x['AS1']), np.array(x['AR']), all_dates), axis=1)
    dataframe['TAM_FLAG'] = dataframe['TAM'].apply(get_TAM_flag)
    dataframe['STABILITY_STATUS'] = dataframe.apply(lambda x: classificar(x['ANUALCYCLE'], x['TAM_FLAG']), axis=1)
    return dataframe


def parametrizar(dataframe:pd.DataFrame) -> None:
    """For a dataframe with the spectral indexes, calculate the umbral values for the TAM, days, and productivity

    Args:
        dataframe (pd.DataFrame): dataframe with the spectral indexes
    
    Returns:
        None     
    """
    import numpy as np
    import os, sys
    global umbral_tam, umbral_productivity, umbral_days
    
    def gaussianfilter(data, sigma = 2, title = None):
        """Applies a gaussian filter to the data. Filters the confidence intervals until is not removed any value.

        Args:
            data (np.array): Data to be filtered.
            sigma (float): Sigma value for the gaussian filter.
        
        Returns:
            tuple: Mean and standard deviation of the filtered data.
        """
        import seaborn as sns
        import matplotlib.pyplot as plt
        done = False
        last_len = len(data)
        n = 0
        while not done:
            mean = np.nanmean(data)
            std = np.nanstd(data)
            data = data[(data > mean - sigma * std) & (data < mean + sigma * std)]
            if len(data) == last_len:
                done = True
            last_len = len(data)
            n += 1
        sns.histplot(data, kde=True)
        plt.axvline(mean, color='r', linestyle='--')
        plt.axvline(mean + 2 * std, color='g', linestyle='--')
        plt.axvline(mean - 2 * std, color='g', linestyle='--')
        if title:
            plt.title(title)
        plt.legend(['Mean', 'Mean + 2*std', 'Mean - 2*std'])
        plt.savefig(f'umbrals/{title}.png')
        plt.close()
        return mean, std, n       

    def getUmbralTAM(tams):
        mean, std, n = gaussianfilter(tams, title='TAM DISTRIBUTION')
        return mean + 2 * std

    def getUmbralDays(days):
        mean, std ,n = gaussianfilter(days, sigma=2.5, title='DAYS DISTRIBUTION')
        return mean

    def getUmbralProductivity(productivity):
        mean, std, n = gaussianfilter(productivity, title='PRODUCTIVITY DISTRIBUTION')
        return mean
    
    dataframe, all_dates = process_series(dataframe)

    dataframe['TAM'] = dataframe['NDVI'].apply(lambda x: __interanualTAM(np.array(x), all_dates))
    dataframe['INTERVALS'] = dataframe.apply(lambda x: __intervals(np.array(x['AS1']), np.array(x['AR']), all_dates), axis=1)
    
    os.makedirs('umbrals', exist_ok=True)
    tams = dataframe['TAM'].values
    tams = np.concatenate(tams)
    umbral_tam = round(getUmbralTAM(tams), 2)
    days = dataframe['INTERVALS'].apply(lambda x: [inter[1] for inter in x]).values
    days = np.concatenate(days)
    umbral_days = round(getUmbralDays(days), 2)
    productivity = dataframe['INTERVALS'].apply(lambda x: [inter[2] for inter in x]).values
    productivity = np.concatenate(productivity)
    umbral_productivity = round(getUmbralProductivity(productivity), 2)
    print(f'UMBRAL TAM: {umbral_tam}\nUMBRAL DAYS: {umbral_days}\nUMBRAL PRODUCTIVITY: {umbral_productivity}')
    umbrals = pd.DataFrame({'UMBRAL_TAM': [umbral_tam], 'UMBRAL_DAYS': [umbral_days], 'UMBRAL_PRODUCTIVITY': [umbral_productivity]})
    umbrals.to_csv('umbrals/umbrals.csv', index=False)

