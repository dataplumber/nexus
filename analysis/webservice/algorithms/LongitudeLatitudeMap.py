"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import math

import numpy as np
from scipy import stats

from webservice.NexusHandler import NexusHandler, nexus_handler, DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NoDataException


#from mpl_toolkits.basemap import Basemap

#TODO Need to update to use nexustiles

@nexus_handler
class LongitudeLatitudeMapHandlerImpl(NexusHandler):

    name = "Longitude/Latitude Time Average Map"
    path = "/longitudeLatitudeMap"
    description = "Computes a Latitude/Longitude Time Average plot given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        NexusHandler.__init__(self)


    def calc(self, computeOptions, **args):
        minLat = computeOptions.get_min_lat()
        maxLat = computeOptions.get_max_lat()
        minLon = computeOptions.get_min_lon()
        maxLon = computeOptions.get_max_lon()
        ds = computeOptions.get_dataset()[0]
        startTime = computeOptions.get_start_time()
        endTime = computeOptions.get_end_time()
        maskLimitType = computeOptions.get_mask_type()

        chunks, meta = self.getChunksForBox(minLat, maxLat, minLon, maxLon, ds, startTime=startTime, endTime=endTime)

        if len(chunks) == 0:
            raise NoDataException(reason="No data found for selected timeframe")

        masker = LandMaskChecker(self._landmask, maskLimitType)
        a = self._allocateArray(int(math.ceil(maxLat - minLat)), int(math.ceil(maxLon - minLon)))
        lat = minLat
        y = 0
        x = 0
        while lat < maxLat:
            lon = minLon
            x = 0
            while lon < maxLon:

                values = []
                # for t in range(0, len(chunks)):
                for n in chunks:

                    chunk = chunks[n]
                    value = chunk.getValueForLatLon(lat, lon)
                    lm = chunk.getLandmaskForLatLon(lat, lon)
                    if lm == 1.0 and value != 32767.0 and not masker.isLatLonMasked(lat, lon):
                        values.append(value)

                if len(values) > 0:
                    avg = np.average(values)
                    min = np.min(values)
                    max = np.max(values)
                    std = np.std(values)
                    cnt = len(values)

                    xi = range(0, len(values))
                    slope, intercept, r_value, p_value, std_err = stats.linregress(xi, values)

                else:
                    avg, min, max, std, cnt = (0, 0, 0, 0, 0)
                    slope, intercept, r_value, p_value, std_err = (0, 0, 0, 0, 0)

                avg = 0.0 if not self._validNumber(float(avg)) else float(avg)
                min = 0.0 if not self._validNumber(float(min)) else float(min)
                max = 0.0 if not self._validNumber(float(max)) else float(max)
                std = 0.0 if not self._validNumber(float(std)) else float(std)
                cnt = 0.0 if not self._validNumber(float(cnt)) else float(cnt)
                slope = 0.0 if not self._validNumber(float(slope)) else float(slope)
                intercept = 0.0 if not self._validNumber(float(intercept)) else float(intercept)
                r_value = 0.0 if not self._validNumber(float(r_value)) else float(r_value)
                p_value = 0.0 if not self._validNumber(float(p_value)) else float(p_value)
                std_err = 0.0 if not self._validNumber(float(std_err)) else float(std_err)

                a[y][x] = {
                    'avg': avg,
                    'min': min,
                    'max': max,
                    'std': std,
                    'cnt': cnt,
                    'slope': slope,
                    'intercept': intercept,
                    'r': r_value,
                    'p': p_value,
                    'stderr': std_err,
                    'lat': float(lat),
                    'lon': float(lon)
                }

                lon = lon + 1
                x = x + 1
            lat = lat + 1
            y = y + 1

        return LongitudeLatitudeMapResults(results=a, meta=meta, computeOptions=computeOptions)


class LongitudeLatitudeMapResults(NexusResults):

    def __init__(self, results=None, meta=None, computeOptions=None):
        NexusResults.__init__(self, results=results, meta=meta, stats=None, computeOptions=computeOptions)

    '''
    def toImage(self):

        res = self.results()
        meta = self.meta()
        startTime = self.computeOptions().get_start_time()
        endTime = self.computeOptions().get_end_time()

        latSeries = [m[0]['lat'] for m in res]
        lonSeries = [m['lon'] for m in res[0]][:]
        data = np.zeros((len(lonSeries), len(latSeries)))
        for t in range(0, len(latSeries)):
            latSet = res[t]
            for l in range(0, len(lonSeries)):
                data[l][t] = latSet[l]['avg']

        data[data == 0.0] = np.nan
        #data = np.rot90(data, 3)
        lats, lons = np.meshgrid(latSeries, lonSeries)
        masked_array = np.ma.array (data, mask=np.isnan(data))
        z = masked_array

        fig = plt.figure()
        fig.set_size_inches(11.0, 8.5)
        ax = fig.add_axes([0.05,0.05,0.9,0.9])


        m = Basemap(projection='ortho',lat_0=20,lon_0=-100,resolution='l')
        raise Exception("Trap")
        #m.drawmapboundary(fill_color='0.3')
        im1 = m.pcolormesh(lons,lats,z,shading='flat',cmap=plt.cm.jet,latlon=True)

        m.drawparallels(np.arange(-90.,99.,30.))
        m.drawmeridians(np.arange(-180.,180.,60.))

        #m.drawcoastlines()
        #m.drawcountries()
        cb = m.colorbar(im1,"bottom", size="5%", pad="2%")

        title = meta['title']
        source = meta['source']
        if startTime is not None and endTime is not None:
            if type(startTime) is not datetime.datetime:
                startTime = datetime.datetime.fromtimestamp(startTime / 1000)
            if type(endTime) is not datetime.datetime:
                endTime = datetime.datetime.fromtimestamp(endTime / 1000)
            dateRange = "%s - %s"%(startTime.strftime('%b %Y'), endTime.strftime('%b %Y'))
        else:
            dateRange = ""

        ax.set_title("%s\n%s\n%s"%(title, source, dateRange))
        ax.set_ylabel('Latitude')
        ax.set_xlabel('Longitude')

        sio = StringIO()
        plt.savefig(sio, format='png')
        return sio.getvalue()
            '''
