"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import logging
import sys
import traceback
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool, Manager

import numpy as np
import pytz
from nexustiles.nexustiles import NexusTileService
from shapely import wkt

from webservice.NexusHandler import NexusHandler, nexus_handler, SparkHandler
from webservice.webmodel import NexusResults, NexusProcessingException

SENTINEL = 'STOP'

EPOCH = pytz.timezone('UTC').localize(datetime(1970, 1, 1))


def iso_time_to_epoch(str_time):
    return (datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=pytz.UTC) - EPOCH).total_seconds()


@nexus_handler
class DailyDifferenceAverageSparkImpl(SparkHandler):
    name = "Daily Difference Average"
    path = "/dailydifferenceaverage_spark"
    description = "Subtracts data in box in Dataset 1 from Dataset 2, then averages the difference per day."
    params = {
        "dataset": {
            "name": "Dataset",
            "type": "string",
            "description": "The Dataset shortname to use in calculation"
        },
        "climatology": {
            "name": "Climatology",
            "type": "string",
            "description": "The Climatology shortname to use in calculation"
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude"
        },
        "startTime": {
            "name": "Start Time",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ"
        },
        "endTime": {
            "name": "End Time",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ"
        }
    }
    singleton = True

    def __init__(self):
        SparkHandler.__init__(self, skipCassandra=True)
        self.log = logging.getLogger(__name__)

    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")
        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            raise NexusProcessingException(
                reason="'b' argument is required. Must be comma-delimited float formatted as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
                code=400)
        dataset = request.get_argument('dataset', None)
        if dataset is None:
            raise NexusProcessingException(reason="'dataset' argument is required", code=400)
        climatology = request.get_argument('climatology', None)
        if climatology is None:
            raise NexusProcessingException(reason="'climatology' argument is required", code=400)

        try:
            start_time = request.get_start_datetime()
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        start_seconds_from_epoch = long((start_time - EPOCH).total_seconds())
        end_seconds_from_epoch = long((end_time - EPOCH).total_seconds())

        return bounding_polygon, dataset, climatology, start_time, start_seconds_from_epoch, end_time, end_seconds_from_epoch

    def calc(self, request, **args):
        bounding_polygon, dataset, climatology, start_time, start_seconds_from_epoch, end_time, end_seconds_from_epoch = self.parse_arguments(
            request)

        self.log.debug("Querying for tiles in search domain")
        # Get tile ids in box
        tile_ids = [tile.tile_id for tile in
                    self._tile_service.find_tiles_in_polygon(bounding_polygon, dataset,
                                                             start_seconds_from_epoch, end_seconds_from_epoch,
                                                             fetch_data=False, fl='id',
                                                             sort=['tile_min_time_dt asc', 'tile_min_lon asc',
                                                                   'tile_min_lat asc'], rows=5000)]

        # Call spark_matchup
        self.log.debug("Calling Spark Driver")
        try:
            spark_result = spark_anomolies_driver(tile_ids, wkt.dumps(bounding_polygon), dataset, climatology,
                                                  sc=self._sc)
        except Exception as e:
            self.log.exception(e)
            raise NexusProcessingException(reason="An unknown error occurred while computing average differences",
                                           code=500)

        averagebyday = spark_result

        result = NexusResults(
            results=[[{'time': dayms * 1000, 'mean': avg, 'ds': 0}] for dayms, avg in averagebyday],
            stats={},
            meta=self.get_meta())

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds[0]
        result.extendMeta(min_lat, max_lat, min_lon, max_lon, "", start_time, end_time)
        result.meta()['label'] = u'Difference from 5-Day mean (\u00B0C)'

        return result

    def date_from_ms(self, dayms):
        base_datetime = datetime(1970, 1, 1)
        delta = timedelta(0, 0, 0, dayms)
        return base_datetime + delta

    def get_meta(self):
        meta = {
            "title": "Sea Surface Temperature (SST) Anomalies",
            "description": "SST anomolies are departures from the 5-day pixel mean",
            "units": u'\u00B0C',
        }
        return meta

    def get_daily_difference_average_for_box(self, min_lat, max_lat, min_lon, max_lon, dataset1, dataset2,
                                             start_time,
                                             end_time):

        daysinrange = self._tile_service.find_days_in_range_asc(min_lat, max_lat, min_lon, max_lon, dataset1,
                                                                start_time, end_time)

        maxprocesses = int(self.algorithm_config.get("multiprocessing", "maxprocesses"))

        if maxprocesses == 1:
            calculator = DailyDifferenceAverageCalculator()
            averagebyday = []
            for dayinseconds in daysinrange:
                result = calculator.calc_average_diff_on_day(min_lat, max_lat, min_lon, max_lon, dataset1, dataset2,
                                                             dayinseconds)
                averagebyday.append((result[0], result[1]))
        else:
            # Create a task to calc average difference for each day
            manager = Manager()
            work_queue = manager.Queue()
            done_queue = manager.Queue()
            for dayinseconds in daysinrange:
                work_queue.put(
                    ('calc_average_diff_on_day', min_lat, max_lat, min_lon, max_lon, dataset1, dataset2, dayinseconds))
            [work_queue.put(SENTINEL) for _ in xrange(0, maxprocesses)]

            # Start new processes to handle the work
            pool = Pool(maxprocesses)
            [pool.apply_async(pool_worker, (work_queue, done_queue)) for _ in xrange(0, maxprocesses)]
            pool.close()

            # Collect the results as [(day (in ms), average difference for that day)]
            averagebyday = []
            for i in xrange(0, len(daysinrange)):
                result = done_queue.get()
                if result[0] == 'error':
                    print >> sys.stderr, result[1]
                    raise NexusProcessingException(reason="Error calculating average by day.")
                rdata = result
                averagebyday.append((rdata[0], rdata[1]))

            pool.terminate()
            manager.shutdown()

        return averagebyday


from threading import Lock
import operator

DRIVER_LOCK = Lock()


def spark_anomolies_driver(self, tile_ids, bounding_wkt, dataset, climatology, sc=None):
    from functools import partial

    with DRIVER_LOCK:
        bounding_wkt_b = sc.broadcast(bounding_wkt)
        dataset_b = sc.broadcast(dataset)
        climatology_b = sc.broadcast(climatology)

        # Parallelize list of tile ids
        rdd = sc.parallelize(tile_ids)

    def add_tuple_elements(tuple1, tuple2):
        tuple(map(operator.add, tuple1, tuple2))

    result = rdd \
        .mapPartitions(partial(calculate_diff, tile_ids=tile_ids, bounding_wkt=bounding_wkt_b, dataset=dataset_b,
                               climatology=climatology_b)) \
        .reduceByKey(add_tuple_elements) \
        .mapValues(operator.div) \
        .sortByKey() \
        .collectAsMap()

    return result


def calculate_diff(tile_ids, bounding_wkt, dataset, climatology):
    tile_ids = list(tile_ids)
    if len(tile_ids) == 0:
        return []
    tile_service = NexusTileService()

    for tile_id in tile_ids:
        # Load the dataset tile
        try:
            the_time = datetime.now()
            dataset_tile = tile_service.mask_tiles_to_polygon(wkt.loads(bounding_wkt.value),
                                                      tile_service.find_tile_by_id(tile_id))[0]
            print "%s Time to load tile %s" % (str(datetime.now() - the_time), tile_id)
        except IndexError:
            # This should only happen if all measurements in a tile become masked after applying the bounding polygon
            raise StopIteration

        # Load the climatology tile
        try:
            the_time = datetime.now()
            dataset_tile = tile_service.mask_tiles_to_polygon(wkt.loads(bounding_wkt.value),
                                                              tile_service.find_tile_by_id(tile_id))[0]
            print "%s Time to load tile %s" % (str(datetime.now() - the_time), tile_id)
        except IndexError:
            # This should only happen if all measurements in a tile become masked after applying the bounding polygon
            raise StopIteration


class DailyDifferenceAverageCalculator(object):
    def __init__(self):
        self.__tile_service = NexusTileService()

    def calc_average_diff_on_day(self, min_lat, max_lat, min_lon, max_lon, dataset1, dataset2, timeinseconds):

        day_of_year = datetime.fromtimestamp(timeinseconds, pytz.utc).timetuple().tm_yday

        ds1_nexus_tiles = self.__tile_service.find_all_tiles_in_box_at_time(min_lat, max_lat, min_lon, max_lon,
                                                                            dataset1,
                                                                            timeinseconds)

        # Initialize list of differences
        differences = []
        # For each ds1tile
        for ds1_tile in ds1_nexus_tiles:
            # Get tile for ds2 using bbox from ds1_tile and day ms
            try:
                ds2_tile = self.__tile_service.find_tile_by_bbox_and_most_recent_day_of_year(ds1_tile.bbox.min_lat,
                                                                                             ds1_tile.bbox.max_lat,
                                                                                             ds1_tile.bbox.min_lon,
                                                                                             ds1_tile.bbox.max_lon,
                                                                                             dataset2, day_of_year)[0]
                # Subtract ds2 tile from ds1 tile
                diff = np.subtract(ds1_tile.data, ds2_tile.data)
            except IndexError:
                # This happens when there is data in ds1tile but all NaNs in ds2tile because the
                # Solr query being used filters out results where stats_count = 0.
                # Technically, this should never happen if ds2 is a climatology generated in part from ds1
                # and it is probably a data error

                # For now, just treat ds2 as an array of all masked data (which essentially discards the ds1 data)
                ds2_tile = np.ma.masked_all(ds1_tile.data.shape)
                diff = np.subtract(ds1_tile.data, ds2_tile)

            # Put results in list of differences
            differences.append(np.ma.array(diff).ravel())

        # Average List of differences
        diffaverage = np.ma.mean(differences).item()
        # Return Average by day
        return int(timeinseconds), diffaverage


def pool_worker(work_queue, done_queue):
    try:
        calculator = DailyDifferenceAverageCalculator()

        for work in iter(work_queue.get, SENTINEL):
            scifunction = work[0]
            args = work[1:]
            result = calculator.__getattribute__(scifunction)(*args)
            done_queue.put(result)
    except Exception as e:
        e_str = traceback.format_exc(e)
        done_queue.put(('error', e_str))
