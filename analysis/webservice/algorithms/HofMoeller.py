"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import itertools
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


def longitude_time_hofmoeller_stats(tile, index):
    stat = {
        'sequence': index,
        'time': np.ma.min(tile.times),
        'lons': []
    }

    points = list(tile.nexus_point_generator())
    data = sorted(points, key=lambda p: p.longitude)
    points_by_lon = itertools.groupby(data, key=lambda p: p.longitude)

    for lon, points_at_lon in points_by_lon:
        values_at_lon = np.array([point.data_val for point in points_at_lon])
        stat['lons'].append({
            'longitude': float(lon),
            'cnt': len(values_at_lon),
            'avg': np.mean(values_at_lon).item(),
            'max': np.max(values_at_lon).item(),
            'min': np.min(values_at_lon).item(),
            'std': np.std(values_at_lon).item()
        })

    return stat


def latitude_time_hofmoeller_stats(tile, index):
    stat = {
        'sequence': index,
        'time': np.ma.min(tile.times),
        'lats': []
    }

    points = list(tile.nexus_point_generator())
    data = sorted(points, key=lambda p: p.latitude)
    points_by_lat = itertools.groupby(data, key=lambda p: p.latitude)

    for lat, points_at_lat in points_by_lat:
        values_at_lat = np.array([point.data_val for point in points_at_lat])

        stat['lats'].append({
            'latitude': float(lat),
            'cnt': len(values_at_lat),
            'avg': np.mean(values_at_lat).item(),
            'max': np.max(values_at_lat).item(),
            'min': np.min(values_at_lat).item(),
            'std': np.std(values_at_lat).item()
        })

    return stat


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
        tiles = self._tile_service.get_tiles_bounded_by_box(computeOptions.get_min_lat(), computeOptions.get_max_lat(),
                                                            computeOptions.get_min_lon(), computeOptions.get_max_lon(),
                                                            computeOptions.get_dataset()[0],
                                                            computeOptions.get_start_time(),
                                                            computeOptions.get_end_time())

        if len(tiles) == 0:
            raise NexusProcessingException.NoDataException(reason="No data found for selected timeframe")

        maxprocesses = int(self.algorithm_config.get("multiprocessing", "maxprocesses"))
        pool = ThreadPool(processes=maxprocesses)
        results = [pool.apply_async(latitude_time_hofmoeller_stats, args=(tile, x)) for x, tile in enumerate(tiles)]
        pool.close()
        pool.join()

        results = [p.get() for p in results]
        results = sorted(results, key=lambda entry: entry["time"])

        results = self.applyDeseasonToHofMoeller(results)

        result = HoffMoellerResults(results=results, compute_options=computeOptions, type=HoffMoellerResults.LATITUDE)
        return result


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
        tiles = self._tile_service.get_tiles_bounded_by_box(computeOptions.get_min_lat(), computeOptions.get_max_lat(),
                                                            computeOptions.get_min_lon(), computeOptions.get_max_lon(),
                                                            computeOptions.get_dataset()[0],
                                                            computeOptions.get_start_time(),
                                                            computeOptions.get_end_time())

        if len(tiles) == 0:
            raise NexusProcessingException.NoDataException(reason="No data found for selected timeframe")

        maxprocesses = int(self.algorithm_config.get("multiprocessing", "maxprocesses"))
        pool = ThreadPool(processes=maxprocesses)
        results = [pool.apply_async(longitude_time_hofmoeller_stats, args=(tile, x)) for x, tile in enumerate(tiles)]
        pool.close()
        pool.join()

        results = [p.get() for p in results]
        results = sorted(results, key=lambda entry: entry["time"])

        results = self.applyDeseasonToHofMoeller(results, pivot="lons")

        result = HoffMoellerResults(results=results, compute_options=computeOptions, type=HoffMoellerResults.LONGITUDE)
        return result


class HoffMoellerResults(NexusResults):
    LATITUDE = 0
    LONGITUDE = 1

    def __init__(self, results=None, meta=None, stats=None, compute_options=None, **args):
        NexusResults.__init__(self, results=results, meta=meta, stats=stats, compute_options=compute_options)
        self.__type = args['type']

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
