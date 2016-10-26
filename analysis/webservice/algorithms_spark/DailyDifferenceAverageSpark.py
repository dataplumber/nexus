"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import logging
from datetime import datetime, timedelta

import numpy as np
import pytz
from nexustiles.nexustiles import NexusTileService
from shapely import wkt

from webservice.NexusHandler import nexus_handler, SparkHandler
from webservice.webmodel import NexusResults, NexusProcessingException

SENTINEL = 'STOP'

EPOCH = pytz.timezone('UTC').localize(datetime(1970, 1, 1))


def iso_time_to_epoch(str_time):
    return (datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=pytz.UTC) - EPOCH).total_seconds()


@nexus_handler
class DailyDifferenceAverageSparkImpl(SparkHandler):
    name = "Daily Difference Average Spark"
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

        min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds
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


from threading import Lock
import operator

DRIVER_LOCK = Lock()


def spark_anomolies_driver(tile_ids, bounding_wkt, dataset, climatology, sc=None):
    from functools import partial

    with DRIVER_LOCK:
        bounding_wkt_b = sc.broadcast(bounding_wkt)
        dataset_b = sc.broadcast(dataset)
        climatology_b = sc.broadcast(climatology)

        # Parallelize list of tile ids
        rdd = sc.parallelize(tile_ids)

    def add_tuple_elements(tuple1, tuple2):
        return tuple(map(operator.add, tuple1, tuple2))

    result = rdd \
        .mapPartitions(partial(calculate_diff, bounding_wkt=bounding_wkt_b, dataset=dataset_b,
                               climatology=climatology_b)) \
        .reduceByKey(add_tuple_elements) \
        .mapValues(lambda x: x[0]/x[1]) \
        .sortByKey() \
        .collect()

    return result


def generate_diff(data_tile, climatology_tile):
    the_time = datetime.now()
    # Subtract climatology tile from data tile
    diff = np.ma.subtract(data_tile.data, climatology_tile.data)
    diff_ct = np.ma.count(diff)
    diff_sum = np.ma.sum(diff)
    date_in_seconds = int((
        datetime.combine(data_tile.min_time.date(), datetime.min.time()).replace(
            tzinfo=pytz.UTC) - EPOCH).total_seconds())

    print "%s Time to generate diff between %s and %s" % (
        str(datetime.now() - the_time), data_tile.tile_id, climatology_tile.tile_id)

    yield (date_in_seconds, (diff_sum, diff_ct))


def calculate_diff(tile_ids, bounding_wkt, dataset, climatology):
    from shapely.geometry import box
    from itertools import chain

    # Construct a list of generators that yield (day, sum, count)
    diff_generators = []

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
            continue

        tile_day_of_year = dataset_tile.min_time.timetuple().tm_yday

        # Load the climatology tile
        try:
            the_time = datetime.now()
            climatology_tile = tile_service.mask_tiles_to_polygon(wkt.loads(bounding_wkt.value),
                                                                  tile_service.find_tile_by_polygon_and_most_recent_day_of_year(
                                                                      box(dataset_tile.bbox.min_lon,
                                                                          dataset_tile.bbox.min_lat,
                                                                          dataset_tile.bbox.max_lon,
                                                                          dataset_tile.bbox.max_lat),
                                                                      climatology.value,
                                                                      tile_day_of_year))[0]
            print "%s Time to load climatology tile %s" % (str(datetime.now() - the_time), tile_id)
        except IndexError:
            # This should only happen if all measurements in a tile become masked after applying the bounding polygon
            continue

        diff_generators.append(generate_diff(dataset_tile, climatology_tile))

    return chain(*diff_generators)
