"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import itertools
import logging
import traceback
from cStringIO import StringIO
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from nexustiles.nexustiles import NexusTileService
from scipy import stats

from webservice import Filtering as filt
from webservice.NexusHandler import nexus_handler, SparkHandler, DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NoDataException, NexusProcessingException

SENTINEL = 'STOP'


@nexus_handler
class TimeSeriesHandlerImpl(SparkHandler):
    name = "Time Series Spark"
    path = "/timeSeriesSpark"
    description = "Computes a time series plot between one or more datasets given an arbitrary geographical area and time range"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        SparkHandler.__init__(self, skipCassandra=True)
        self.log = logging.getLogger(__name__)

    def calc(self, computeOptions, **args):
        """

        :param computeOptions: StatsComputeOptions
        :param args: dict
        :return:
        """

        ds = computeOptions.get_dataset()

        if type(ds) != list and type(ds) != tuple:
            ds = (ds,)

        if next(iter([clim for clim in ds if 'CLIM' in clim]), False):
            raise NexusProcessingException(reason="Cannot compute time series on a climatology", code=400)

        resultsRaw = []

        spark_master, spark_nexecs, spark_nparts = computeOptions.get_spark_cfg()
        for shortName in ds:
            results, meta = self.getTimeSeriesStatsForBoxSingleDataSet(computeOptions.get_min_lat(),
                                                                       computeOptions.get_max_lat(),
                                                                       computeOptions.get_min_lon(),
                                                                       computeOptions.get_max_lon(),
                                                                       shortName,
                                                                       computeOptions.get_start_time(),
                                                                       computeOptions.get_end_time(),
                                                                       computeOptions.get_apply_seasonal_cycle_filter(),
                                                                       computeOptions.get_apply_low_pass_filter(),
                                                                       spark_master=spark_master,
                                                                       spark_nexecs=spark_nexecs,
                                                                       spark_nparts=spark_nparts)
            resultsRaw.append([results, meta])

        results = self._mergeResults(resultsRaw)

        if len(ds) == 2:
            try:
                stats = self.calculateComparisonStats(results, suffix="")
            except Exception:
                stats = {}
                tb = traceback.format_exc()
                self.log.warn("Error when calculating comparison stats:\n%s" % tb)
            if computeOptions.get_apply_seasonal_cycle_filter():
                try:
                    s = self.calculateComparisonStats(results, suffix="Seasonal")
                    stats = self._mergeDicts(stats, s)
                except Exception:
                    tb = traceback.format_exc()
                    self.log.warn("Error when calculating Seasonal comparison stats:\n%s" % tb)
            if computeOptions.get_apply_low_pass_filter():
                try:
                    s = self.calculateComparisonStats(results, suffix="LowPass")
                    stats = self._mergeDicts(stats, s)
                except Exception:
                    tb = traceback.format_exc()
                    self.log.warn("Error when calculating LowPass comparison stats:\n%s" % tb)
            if computeOptions.get_apply_seasonal_cycle_filter() and computeOptions.get_apply_low_pass_filter():
                try:
                    s = self.calculateComparisonStats(results, suffix="SeasonalLowPass")
                    stats = self._mergeDicts(stats, s)
                except Exception:
                    tb = traceback.format_exc()
                    self.log.warn("Error when calculating SeasonalLowPass comparison stats:\n%s" % tb)
        else:
            stats = {}

        meta = []
        for singleRes in resultsRaw:
            meta.append(singleRes[1])

        res = TimeSeriesResults(results=results, meta=meta, stats=stats,
                                computeOptions=computeOptions)
        return res

    def getTimeSeriesStatsForBoxSingleDataSet(self, min_lat, max_lat,
                                              min_lon, max_lon, ds,
                                              start_time=0, end_time=-1,
                                              applySeasonalFilter=True,
                                              applyLowPass=True,
                                              fill=-9999.,
                                              spark_master="local[1]",
                                              spark_nexecs=1,
                                              spark_nparts=1):

        daysinrange = self._tile_service.find_days_in_range_asc(min_lat,
                                                                max_lat,
                                                                min_lon,
                                                                max_lon, ds,
                                                                start_time,
                                                                end_time)

        ndays = len(daysinrange)
        if ndays == 0:
            raise NoDataException(reason="No data found for selected timeframe")

        self.log.debug('Found {0} days in range'.format(ndays))
        for i, d in enumerate(daysinrange):
            self.log.debug('{0}, {1}'.format(i, datetime.utcfromtimestamp(d)))
        spark_nparts_needed = min(spark_nparts, ndays)
        nexus_tiles_spark = [(min_lat, max_lat, min_lon, max_lon, ds,
                              list(daysinrange_part), fill)
                             for daysinrange_part
                             in np.array_split(daysinrange, 
                                               spark_nparts_needed)]

        # Launch Spark computations
        rdd = self._sc.parallelize(nexus_tiles_spark, spark_nparts_needed)
        results = rdd.map(TimeSeriesCalculator.calc_average_on_day).collect()
        #
        results = list(itertools.chain.from_iterable(results))
        results = sorted(results, key=lambda entry: entry["time"])

        filt.applyAllFiltersOnField(results, 'mean', applySeasonal=applySeasonalFilter, applyLowPass=applyLowPass)
        filt.applyAllFiltersOnField(results, 'max', applySeasonal=applySeasonalFilter, applyLowPass=applyLowPass)
        filt.applyAllFiltersOnField(results, 'min', applySeasonal=applySeasonalFilter, applyLowPass=applyLowPass)

        self._create_nc_file_time1d(np.array(results), 'ts.nc', 'mean',
                                    fill=-9999.)
        return results, {}

    def calculateComparisonStats(self, results, suffix=""):

        xy = [[], []]

        for item in results:
            if len(item) == 2:
                xy[item[0]["ds"]].append(item[0]["mean%s" % suffix])
                xy[item[1]["ds"]].append(item[1]["mean%s" % suffix])

        slope, intercept, r_value, p_value, std_err = stats.linregress(xy[0], xy[1])
        comparisonStats = {
            "slope%s" % suffix: slope,
            "intercept%s" % suffix: intercept,
            "r%s" % suffix: r_value,
            "p%s" % suffix: p_value,
            "err%s" % suffix: std_err
        }

        return comparisonStats


class TimeSeriesResults(NexusResults):
    LINE_PLOT = "line"
    SCATTER_PLOT = "scatter"

    __SERIES_COLORS = ['red', 'blue']

    def __init__(self, results=None, meta=None, stats=None, computeOptions=None):
        NexusResults.__init__(self, results=results, meta=meta, stats=stats, computeOptions=computeOptions)

    def toImage(self):

        type = self.computeOptions().get_plot_type()

        if type == TimeSeriesResults.LINE_PLOT or type == "default":
            return self.createLinePlot()
        elif type == TimeSeriesResults.SCATTER_PLOT:
            return self.createScatterPlot()
        else:
            raise Exception("Invalid or unsupported time series plot specified")

    def createScatterPlot(self):
        timeSeries = []
        series0 = []
        series1 = []

        res = self.results()
        meta = self.meta()

        plotSeries = self.computeOptions().get_plot_series() if self.computeOptions is not None else None
        if plotSeries is None:
            plotSeries = "mean"

        for m in res:
            if len(m) == 2:
                timeSeries.append(datetime.fromtimestamp(m[0]["time"] / 1000))
                series0.append(m[0][plotSeries])
                series1.append(m[1][plotSeries])

        title = ', '.join(set([m['title'] for m in meta]))
        sources = ', '.join(set([m['source'] for m in meta]))
        dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

        fig, ax = plt.subplots()
        fig.set_size_inches(11.0, 8.5)
        ax.scatter(series0, series1, alpha=0.5)
        ax.set_xlabel(meta[0]['units'])
        ax.set_ylabel(meta[1]['units'])
        ax.set_title("%s\n%s\n%s" % (title, sources, dateRange))

        par = np.polyfit(series0, series1, 1, full=True)
        slope = par[0][0]
        intercept = par[0][1]
        xl = [min(series0), max(series0)]
        yl = [slope * xx + intercept for xx in xl]
        plt.plot(xl, yl, '-r')

        # r = self.stats()["r"]
        # plt.text(0.5, 0.5, "r = foo")

        ax.grid(True)
        fig.tight_layout()

        sio = StringIO()
        plt.savefig(sio, format='png')
        return sio.getvalue()

    def createLinePlot(self):
        nseries = len(self.meta())
        res = self.results()
        meta = self.meta()

        timeSeries = [datetime.fromtimestamp(m[0]["time"] / 1000) for m in res]

        means = [[np.nan] * len(res) for n in range(0, nseries)]

        plotSeries = self.computeOptions().get_plot_series() if self.computeOptions is not None else None
        if plotSeries is None:
            plotSeries = "mean"

        for n in range(0, len(res)):
            timeSlot = res[n]
            for seriesValues in timeSlot:
                means[seriesValues['ds']][n] = seriesValues[plotSeries]

        x = timeSeries

        fig, axMain = plt.subplots()
        fig.set_size_inches(11.0, 8.5)
        fig.autofmt_xdate()

        title = ', '.join(set([m['title'] for m in meta]))
        sources = ', '.join(set([m['source'] for m in meta]))
        dateRange = "%s - %s" % (timeSeries[0].strftime('%b %Y'), timeSeries[-1].strftime('%b %Y'))

        axMain.set_title("%s\n%s\n%s" % (title, sources, dateRange))
        axMain.set_xlabel('Date')
        axMain.grid(True)
        axMain.xaxis.set_major_locator(mdates.YearLocator())
        axMain.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        axMain.xaxis.set_minor_locator(mdates.MonthLocator())
        axMain.format_xdata = mdates.DateFormatter('%Y-%m-%d')

        plots = []

        for n in range(0, nseries):
            if n == 0:
                ax = axMain
            else:
                ax = ax.twinx()

            plots += ax.plot(x, means[n], color=self.__SERIES_COLORS[n], zorder=10, linewidth=3, label=meta[n]['title'])
            ax.set_ylabel(meta[n]['units'])

        labs = [l.get_label() for l in plots]
        axMain.legend(plots, labs, loc=0)

        sio = StringIO()
        plt.savefig(sio, format='png')
        return sio.getvalue()


class TimeSeriesCalculator(SparkHandler):
    def __init__(self):
        self.__tile_service = NexusTileService()

    @staticmethod
    def calc_average_on_day(tile_in_spark):
        (min_lat, max_lat, min_lon, max_lon, dataset,
         timestamps, fill) = tile_in_spark
        if len(timestamps) == 0:
            return []
        start_time = timestamps[0]
        end_time = timestamps[-1]
        tile_service = NexusTileService()
        # ds1_nexus_tiles = \
        #    tile_service.get_tiles_bounded_by_box_at_time(min_lat, max_lat, 
        #                                                  min_lon, max_lon, 
        #                                                  dataset, 
        #                                                  timeinseconds)
        ds1_nexus_tiles = \
            tile_service.get_tiles_bounded_by_box(min_lat, max_lat,
                                                  min_lon, max_lon,
                                                  dataset,
                                                  timestamps[0],
                                                  timestamps[-1])
        # ds1_nexus_tiles = TimeSeriesCalculator.query_by_parts(tile_service,
        #                                                      min_lat, 
        #                                                      max_lat, 
        #                                                      min_lon, 
        #                                                      max_lon, 
        #                                                      dataset, 
        #                                                      timestamps[0],
        #                                                      timestamps[-1])

        stats_arr = []
        for timeinseconds in timestamps:
            tile_data_agg = np.ma.array([tile.data.flatten() \
                                         for tile in ds1_nexus_tiles \
                                         if (tile.times[0] == timeinseconds)])
            lats_agg = np.array([np.repeat(tile.latitudes,
                                           len(tile.longitudes))
                                 for tile in ds1_nexus_tiles
                                 if (tile.times[0] ==
                                     timeinseconds)])
            if (len(tile_data_agg) == 0) or tile_data_agg.mask.all():
                continue
            else:
                data_min = np.ma.min(tile_data_agg)
                data_max = np.ma.max(tile_data_agg)
                # daily_mean = np.ma.mean(tile_data_agg).item()
                daily_mean = \
                    np.ma.average(tile_data_agg,
                                  weights=np.cos(np.radians(lats_agg))).item()
                data_count = np.ma.count(tile_data_agg)
                data_std = np.ma.std(tile_data_agg)

                # Return Stats by day
            stat = {
                'min': data_min,
                'max': data_max,
                'mean': daily_mean,
                'cnt': data_count,
                'std': data_std,
                'time': int(timeinseconds)
            }
            stats_arr.append(stat)
        return stats_arr


def pool_worker(work_queue, done_queue):
    try:
        calculator = TimeSeriesCalculator()

        for work in iter(work_queue.get, SENTINEL):
            scifunction = work[0]
            args = work[1:]
            result = calculator.__getattribute__(scifunction)(*args)
            done_queue.put(result)
    except Exception as e:
        e_str = traceback.format_exc(e)
        done_queue.put({'error': e_str})
