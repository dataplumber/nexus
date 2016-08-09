"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from cStringIO import StringIO
from datetime import datetime
from multiprocessing.pool import ThreadPool

import matplotlib.pyplot as plt
import mpld3
import numpy as np
from matplotlib import cm
from matplotlib.ticker import FuncFormatter

from webservice.NexusHandler import NexusHandler, nexus_handler, DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusProcessingException, NexusResults


# TODO Need to update to use nexustiles

def latitudeTimeHofMoellerStatsForChunk(chunk, minLat, maxLat, minLon, maxLon, t, masker=None):
    timeMillis = (int(chunk.start.strftime("%s")) * 1000)

    lats = []
    lat = minLat
    while lat < maxLat:
        lon = minLon

        ttl = 0
        cnt = 0
        max = -100000.0
        min = 100000.0
        std = 0
        while lon < maxLon:
            value = chunk.getValueForLatLon(lat, lon)
            lm = chunk.getLandmaskForLatLon(lat, lon)
            if lm == 1.0 and value != 32767.0 and not masker.isLatLonMasked(lat, lon):
                ttl = ttl + value
                cnt = cnt + 1
                max = max if value < max else value
                min = min if value > min else value
            lon = lon + 1
        if cnt > 0:
            mean = ttl / cnt
        else:
            mean = 0
            min = 0
            max = 0
            std = 0

        latStats = {
            'latitude': float(lat),
            'cnt': float(cnt),
            'avg': float(mean),
            'max': float(max),
            'min': float(min),
            'std': float(std)
        }
        lats.append(latStats)
        lat = lat + 1
    timeAxis = {
        'sequence': t,
        'time': timeMillis,
        'lats': lats
    }
    return timeAxis


def longitudeTimeHofMoellerStatsForChunk(chunk, minLat, maxLat, minLon, maxLon, t, masker=None):
    timeMillis = (int(chunk.start.strftime("%s")) * 1000)

    lons = []
    lon = minLon
    while lon < maxLon:
        lat = minLat

        ttl = 0
        cnt = 0
        max = -100000.0
        min = 100000.0
        std = 0
        while lat < maxLat:
            value = chunk.getValueForLatLon(lat, lon)
            lm = chunk.getLandmaskForLatLon(lat, lon)
            if lm == 1.0 and value != 32767.0 and not masker.isLatLonMasked(lat, lon):
                ttl = ttl + value
                cnt = cnt + 1
                max = max if value < max else value
                min = min if value > min else value
            lat = lat + 1
        if cnt > 0:
            mean = ttl / cnt
        else:
            mean = 0
            min = 0
            max = 0
            std = 0

        lonStats = {
            'longitude': float(lon),
            'cnt': float(cnt),
            'avg': float(mean),
            'max': float(max),
            'min': float(min),
            'std': float(std)
        }
        lons.append(lonStats)
        lon = lon + 1
    timeAxis = {
        'sequence': t,
        'time': timeMillis,
        'lons': lons
    }
    return timeAxis


class BaseHoffMoellerHandlerImpl(NexusHandler):
    def __init__(self):
        NexusHandler.__init__(self)

    def applyDeseasonToHofMoellerByField(self, results, pivot="lats", field="avg", append=True):
        shape = (len(results), len(results[0][pivot]))
        if shape[0] <= 12:
            return results
        for a in range(0, 12):
            values = []
            for b in range(a, len(results), 12):
                values.append(np.average([l[field] for l in results[b][pivot]]))
            avg = np.average(values)

            for b in range(a, len(results), 12):
                for l in results[b][pivot]:
                    l["%sSeasonal" % field] = l[field] - avg

        return results

    def applyDeseasonToHofMoeller(self, results, pivot="lats", append=True):
        results = self.applyDeseasonToHofMoellerByField(results, pivot, field="avg", append=append)
        results = self.applyDeseasonToHofMoellerByField(results, pivot, field="min", append=append)
        results = self.applyDeseasonToHofMoellerByField(results, pivot, field="max", append=append)
        return results


@nexus_handler
class LatitudeTimeHoffMoellerHandlerImpl(BaseHoffMoellerHandlerImpl):
    name = "Latitude/Time HofMoeller"
    path = "/latitudeTimeHofMoeller"
    description = "Computes a latitude/time HofMoeller plot given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        BaseHoffMoellerHandlerImpl.__init__(self)

    def calc(self, computeOptions, **args):
        chunks, meta = self.getChunksForBox(computeOptions.get_min_lat(),
                                            computeOptions.get_max_lat(),
                                            computeOptions.get_min_lon(),
                                            computeOptions.get_max_lon(),
                                            computeOptions.get_dataset()[0],
                                            startTime=computeOptions.get_start_time(),
                                            endTime=computeOptions.get_end_time())

        if len(chunks) == 0:
            raise NexusProcessingException.NoDataException(reason="No data found for selected timeframe")

        masker = LandMaskChecker(self._landmask, computeOptions.get_mask_type())

        # pool = mp.Pool(processes=8)
        pool = ThreadPool(processes=8)
        results = [pool.apply_async(latitudeTimeHofMoellerStatsForChunk,
                                    args=(chunks[x], computeOptions.get_min_lat(), computeOptions.get_max_lat(),
                                          computeOptions.get_min_lon(), computeOptions.get_max_lon(), x, masker)) for x
                   in chunks]
        pool.close()
        pool.join()

        results = [p.get() for p in results]
        results = sorted(results, key=lambda entry: entry["time"])

        results = self.applyDeseasonToHofMoeller(results)
        return HoffMoellerResults(results=results, meta=meta, computeOptions=computeOptions,
                                  type=HoffMoellerResults.LATITUDE)


@nexus_handler
class LongitudeTimeHoffMoellerHandlerImpl(BaseHoffMoellerHandlerImpl):
    name = "Longitude/Time HofMoeller"
    path = "/longitudeTimeHofMoeller"
    description = "Computes a longitude/time HofMoeller plot given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        BaseHoffMoellerHandlerImpl.__init__(self)

    def calc(self, computeOptions, **args):
        chunks, meta = self.getChunksForBox(computeOptions.get_min_lat(),
                                            computeOptions.get_max_lat(),
                                            computeOptions.get_min_lon(),
                                            computeOptions.get_max_lon(),
                                            computeOptions.get_dataset()[0],
                                            startTime=computeOptions.get_start_time(),
                                            endTime=computeOptions.get_end_time())

        if len(chunks) == 0:
            raise NexusProcessingException.NoDataException(reason="No data found for selected timeframe")

        masker = LandMaskChecker(self._landmask, computeOptions.get_mask_type())
        # pool = mp.Pool(processes=8)
        pool = ThreadPool(processes=8)
        results = [pool.apply_async(longitudeTimeHofMoellerStatsForChunk,
                                    args=(chunks[x], computeOptions.get_min_lat(), computeOptions.get_max_lat(),
                                          computeOptions.get_min_lon(), computeOptions.get_max_lon(), x, masker)) for x
                   in chunks]
        pool.close()
        pool.join()

        results = [p.get() for p in results]
        results = sorted(results, key=lambda entry: entry["time"])

        results = self.applyDeseasonToHofMoeller(results, pivot="lons")

        return HoffMoellerResults(results=results, meta=meta, computeOptions=computeOptions,
                                  type=HoffMoellerResults.LONGITUDE)


class HoffMoellerResults(NexusResults):
    LATITUDE = 0
    LONGITUDE = 1

    def __init__(self, results=None, meta=None, computeOptions=None, type=0):
        NexusResults.__init__(self, results=results, meta=meta, stats=None, computeOptions=computeOptions)
        self.__type = type

    def createHoffmueller(self, data, coordSeries, timeSeries, coordName, title, interpolate='nearest'):
        cmap = cm.coolwarm
        # ls = LightSource(315, 45)
        # rgb = ls.shade(data, cmap)

        fig, ax = plt.subplots()
        fig.set_size_inches(11.0, 8.5)
        cax = ax.imshow(data, interpolation=interpolate, cmap=cmap)

        def yFormatter(y, pos):
            if y < len(coordSeries):
                return "%s $^\circ$" % (int(coordSeries[int(y)] * 100.0) / 100.)
            else:
                return ""

        def xFormatter(x, pos):
            if x < len(timeSeries):
                return timeSeries[int(x)].strftime('%b %Y')
            else:
                return ""

        ax.xaxis.set_major_formatter(FuncFormatter(xFormatter))
        ax.yaxis.set_major_formatter(FuncFormatter(yFormatter))

        ax.set_title(title)
        ax.set_ylabel(coordName)
        ax.set_xlabel('Date')

        fig.colorbar(cax)
        fig.autofmt_xdate()

        labels = ['point {0}'.format(i + 1) for i in range(len(data))]
        # plugins.connect(fig, plugins.MousePosition(fontsize=14))
        tooltip = mpld3.plugins.PointLabelTooltip(cax, labels=labels)

        sio = StringIO()
        plt.savefig(sio, format='png')
        return sio.getvalue()

    def createLongitudeHoffmueller(self, res, meta):
        lonSeries = [m['longitude'] for m in res[0]['lons']]
        timeSeries = [datetime.fromtimestamp(m['time'] / 1000) for m in res]

        data = np.zeros((len(lonSeries), len(timeSeries)))

        plotSeries = self.computeOptions().get_plot_series(default="avg") if self.computeOptions is not None else None
        if plotSeries is None:
            plotSeries = "avg"

        for t in range(0, len(timeSeries)):
            timeSet = res[t]
            for l in range(0, len(lonSeries)):
                latSet = timeSet['lons'][l]
                value = latSet[plotSeries]
                data[len(lonSeries) - l - 1][t] = value

        title = meta['title']
        source = meta['source']
        dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

        return self.createHoffmueller(data, lonSeries, timeSeries, "Longitude",
                                      "%s\n%s\n%s" % (title, source, dateRange), interpolate='nearest')

    def createLatitudeHoffmueller(self, res, meta):
        latSeries = [m['latitude'] for m in res[0]['lats']]
        timeSeries = [datetime.fromtimestamp(m['time'] / 1000) for m in res]

        data = np.zeros((len(latSeries), len(timeSeries)))

        plotSeries = self.computeOptions().get_plot_series(default="avg") if self.computeOptions is not None else None
        if plotSeries is None:
            plotSeries = "avg"

        for t in range(0, len(timeSeries)):
            timeSet = res[t]
            for l in range(0, len(latSeries)):
                latSet = timeSet['lats'][l]
                value = latSet[plotSeries]
                data[len(latSeries) - l - 1][t] = value

        title = meta['title']
        source = meta['source']
        dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

        return self.createHoffmueller(data, latSeries, timeSeries, "Latitude",
                                      title="%s\n%s\n%s" % (title, source, dateRange), interpolate='nearest')

    def toImage(self):
        res = self.results()
        meta = self.meta()

        if self.__type == HoffMoellerResults.LATITUDE:
            return self.createLatitudeHoffmueller(res, meta)
        elif self.__type == HoffMoellerResults.LONGITUDE:
            return self.createLongitudeHoffmueller(res, meta)
        else:
            raise Exception("Unsupported HoffMoeller Plot Type")
