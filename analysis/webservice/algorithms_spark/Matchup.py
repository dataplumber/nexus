"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import time
from datetime import datetime

import numpy as np
import requests
import utm
import logging
from nexustiles.nexustiles import NexusTileService
from pyspark import SparkContext, SparkConf
from pytz import timezone, UTC
from scipy import spatial
from shapely import wkt
from shapely.geometry import Point
from webservice.NexusHandler import NexusHandler, nexus_handler
from webservice.webmodel import NexusProcessingException
from webservice.algorithms.doms import values as doms_values
from webservice.algorithms.doms.BaseDomsHandler import DomsQueryResults
from webservice.algorithms.doms.ResultsStorage import ResultsStorage

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))


# TODO Better handling of time tolerance and depth tolerance
@nexus_handler
class Matchup(NexusHandler):
    name = "Matchup"
    path = "/match_spark"
    description = "Match measurements between two or more datasets"

    params = {
        "primary": {
            "name": "Primary Dataset",
            "type": "string",
            "description": "The Primary dataset used to find matches for"
        },
        "matchup": {
            "name": "Match-Up Datasets",
            "type": "comma-delimited string",
            "description": "The Dataset(s) being searched for measurements that match the measurements in Primary"
        },
        "parameter": {
            "name": "Match-Up Parameter",
            "type": "string",
            "description": "The parameter of interest used for the match up. One of 'sst', 'sss', 'wind'."
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
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude"
        },
        "tt": {
            "name": "Time Tolerance",
            "type": "long",
            "description": "Tolerance in time (seconds) when comparing two measurements."
        },
        "dt": {
            "name": "Depth Tolerance",
            "type": "float",
            "description": "Tolerance in depth when comparing two measurements"
        },
        "rt": {
            "name": "Radius Tolerance",
            "type": "float",
            "description": "Tolerance in radius when comparing two measurements"
        },
        "platforms": {
            "name": "Platforms",
            "type": "comma-delimited integer",
            "description": "Platforms to include for matchup consideration"
        }
    }
    singleton = True

    def __init__(self):
        NexusHandler.__init__(self, skipCassandra=True)
        self.log = logging.getLogger(__name__)

    def calc(self, request, **args):
        start = int(round(time.time() * 1000))
        # TODO Assuming Satellite primary
        # Parse input arguments
        self.log.debug("Parsing arguments")
        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            raise NexusProcessingException(
                reason="'b' argument is required. Must be comma-delimited float formatted as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
                code=400)
        primary_ds_name = request.get_argument('primary', None)
        if primary_ds_name is None:
            raise NexusProcessingException(reason="'primary' argument is required", code=400)
        matchup_ds_names = request.get_argument('matchup', None)
        if matchup_ds_names is None:
            raise NexusProcessingException(reason="'matchup' argument is required", code=400)
        parameter_s = request.get_argument('parameter', 'sst')
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
        time_tolerance = request.get_int_arg('tt', default=86400)
        depth_tolerance = request.get_decimal_arg('dt', default=5.0)
        radius_tolerance = request.get_decimal_arg('rt', default=1000.0)
        platforms = request.get_argument('platforms', None)
        if platforms is None:
            raise NexusProcessingException(reason="'platforms' argument is required", code=400)

        start_seconds_from_epoch = long((start_time - EPOCH).total_seconds())
        end_seconds_from_epoch = long((end_time - EPOCH).total_seconds())

        self.log.debug("Querying for tiles in search domain")
        # Get tile ids in box
        tile_ids = [tile['id'] for tile in
                    self._tile_service.find_tiles_in_polygon(bounding_polygon, primary_ds_name,
                                                             start_seconds_from_epoch, end_seconds_from_epoch,
                                                             fetch_data=False, fl='id')]

        self.log.debug("Calling Spark Driver")
        # Call spark_matchup
        spark_result = spark_matchup_driver(tile_ids, wkt.dumps(bounding_polygon), primary_ds_name, matchup_ds_names,
                                            parameter_s, time_tolerance, depth_tolerance, radius_tolerance, platforms)

        end = int(round(time.time() * 1000))

        self.log.debug("Building and saving results")
        args = {
            "primary": primary_ds_name,
            "matchup": matchup_ds_names,
            "startTime": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "endTime": end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "bbox": request.get_argument('b'),
            "timeTolerance": time_tolerance,
            "depthTolerance": float(depth_tolerance),
            "radiusTolerance": float(radius_tolerance),
            "platforms": platforms
        }

        details = {
            "timeToComplete": (end - start),
            "numInSituRecords": 0,
            "numInSituMatched": 0,
            "numGriddedChecked": 0,
            "numGriddedMatched": 0
        }

        matches = self.convert_to_matches(spark_result)

        resultsStorage = ResultsStorage()
        result_id = resultsStorage.insertResults(results=matches, params=args, stats=details, startTime=start,
                                                 completeTime=end, userEmail="")

        return DomsQueryResults(results=matches, args=args, details=details, bounds=None, count=None,
                                computeOptions=None, executionId=result_id)

    @classmethod
    def convert_to_matches(cls, spark_result):
        matches = []
        for primary_domspoint, matched_domspoints in spark_result.iteritems():
            p_matched = [cls.domspoint_to_dict(p_match) for p_match in matched_domspoints]

            primary = cls.domspoint_to_dict(primary_domspoint)
            primary['matches'] = list(p_matched)
            matches.append(primary)
        return matches

    @staticmethod
    def domspoint_to_dict(domspoint):
        return {
            "sea_water_temperature": domspoint.sst,
            "sea_water_temperature_depth": domspoint.sst_depth,
            "sea_water_salinity": domspoint.sss,
            "sea_water_salinity_depth": domspoint.sss_depth,
            "wind_speed": domspoint.wind_speed,
            "wind_direction": domspoint.wind_direction,
            "wind_u": domspoint.wind_u,
            "wind_v": domspoint.wind_v,
            "platform": doms_values.getPlatformById(domspoint.platform),
            "device": doms_values.getDeviceById(domspoint.device),
            "x": domspoint.longitude,
            "y": domspoint.latitude,
            "point": "Point(%s %s)" % (domspoint.longitude, domspoint.latitude),
            "time": (datetime.strptime(domspoint.time, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=UTC) - EPOCH).total_seconds(),
            "fileurl": domspoint.file_url,
            "id": domspoint.data_id,
            "source": domspoint.source,
        }


class DomsPoint(object):
    def __init__(self, longitude=None, latitude=None, time=None, depth=None, data_id=None):

        self.time = time
        self.longitude = longitude
        self.latitude = latitude
        self.depth = depth
        self.data_id = data_id

        if latitude is not None and longitude is not None:
            easting, northing, zone_number, zone_letter = utm.from_latlon(latitude, longitude)
            self.sh_point = Point(easting, northing)
        else:
            self.sh_point = None

        self.wind_u = None
        self.wind_v = None
        self.wind_direction = None
        self.wind_speed = None
        self.sst = None
        self.sst_depth = None
        self.sss = None
        self.sss_depth = None
        self.source = None
        self.depth = None
        self.platform = None
        self.device = None
        self.file_url = None

    def __repr__(self):
        return str(self.__dict__)

    @staticmethod
    def from_nexus_point(nexus_point, tile=None, parameter='sst'):
        point = DomsPoint()

        point.data_id = "%s[%s]" % (tile.tile_id, nexus_point.index)

        # TODO Not an ideal solution; but it works for now.
        if parameter == 'sst':
            point.sst = nexus_point.data_val.item()
        elif parameter == 'sss':
            point.sss = nexus_point.data_val.item()
        elif parameter == 'wind':
            point.wind_u = nexus_point.data_val.item()
            try:
                point.wind_v = tile.meta_data['wind_v'][nexus_point.index].item()
            except (KeyError, IndexError):
                pass
            try:
                point.wind_direction = tile.meta_data['wind_direction'][nexus_point.index].item()
            except (KeyError, IndexError):
                pass
            try:
                point.wind_speed = tile.meta_data['wind_speed'][nexus_point.index].item()
            except (KeyError, IndexError):
                pass
        else:
            raise NotImplementedError('%s not supported. Only sst, sss, and wind parameters are supported.' % parameter)

        point.longitude = nexus_point.longitude.item()
        point.latitude = nexus_point.latitude.item()
        easting, northing, zone_number, zone_letter = utm.from_latlon(point.latitude, point.longitude)
        point.sh_point = Point(easting, northing)

        point.time = datetime.utcfromtimestamp(nexus_point.time).strftime('%Y-%m-%dT%H:%M:%SZ')

        try:
            point.depth = nexus_point.depth
        except KeyError:
            # No depth associated with this measurement
            pass

        point.sst_depth = 0
        point.source = tile.dataset

        # TODO device should change based on the satellite making the observations.
        point.platform = 9
        point.device = 5
        return point

    @staticmethod
    def from_edge_point(edge_point):
        from shapely import wkt
        from shapely.geos import ReadingError
        from shapely.geometry import Point

        point = DomsPoint()

        try:
            x, y = wkt.loads(edge_point['point']).coords[0]
        except ReadingError:
            try:
                x, y = Point(*[float(c) for c in edge_point['point'].split(' ')]).coords[0]
            except ValueError:
                y, x = Point(*[float(c) for c in edge_point['point'].split(',')]).coords[0]

        point.longitude = x
        point.latitude = y
        easting, northing, zone_number, zone_letter = utm.from_latlon(point.latitude, point.longitude)
        point.sh_point = Point(easting, northing)

        point.time = edge_point['time']

        point.wind_u = edge_point.get('wind_u')
        point.wind_v = edge_point.get('wind_v')
        point.wind_direction = edge_point.get('wind_direction')
        point.wind_speed = edge_point.get('wind_speed')
        point.sst = edge_point.get('sea_water_temperature')
        point.sst_depth = edge_point.get('sea_water_temperature_depth')
        point.sss = edge_point.get('sea_water_salinity')
        point.sss_depth = edge_point.get('sea_water_salinity_depth')
        point.source = edge_point.get('source')
        point.platform = edge_point.get('platform')
        point.device = edge_point.get('device')
        point.file_url = edge_point.get('fileurl')

        try:
            point.data_id = unicode(edge_point['id'])
        except KeyError:
            point.data_id = "%s:%s:%s" % (point.time, point.longitude, point.latitude)

        return point


def spark_matchup_driver(tile_ids, bounding_wkt, primary_ds_name, matchup_ds_names, parameter, time_tolerance,
                         depth_tolerance, radius_tolerance, platforms):
    from functools import partial

    # Configure Spark
    sp_conf = SparkConf()
    sp_conf.setAppName("Spark Matchup")

    try:
        sc = SparkContext(conf=sp_conf)
    except ValueError:
        raise NexusProcessingException(reason="Only one spark_matchup can be run at time. Please try again later.",
                                       code=503)

    # TODO Better handling of the Spark context. Do we really need to create/shutdown on every request?
    try:
        # Broadcast parameters
        primary_b = sc.broadcast(primary_ds_name)
        matchup_b = sc.broadcast(matchup_ds_names)
        tt_b = sc.broadcast(time_tolerance)
        dt_b = sc.broadcast(depth_tolerance)
        rt_b = sc.broadcast(radius_tolerance)
        platforms_b = sc.broadcast(platforms)
        bounding_wkt_b = sc.broadcast(bounding_wkt)
        parameter_b = sc.broadcast(parameter)

        # Parallelize list of tile ids
        rdd = sc.parallelize(tile_ids)

        # Map Partitions ( list(tile_id) )
        rdd = rdd.mapPartitions(
            partial(match_satellite_to_insitu, primary_b=primary_b, matchup_b=matchup_b, parameter_b=parameter_b,
                    tt_b=tt_b,
                    dt_b=dt_b, rt_b=rt_b, platforms_b=platforms_b, bounding_wkt_b=bounding_wkt_b),
            preservesPartitioning=True) \
            .combineByKey(lambda value: [value],  # Create 1 element list
                          lambda value_list, value: value_list + [value],  # Add 1 element to list
                          lambda value_list_a, value_list_b: value_list_a + value_list_b)  # Add two lists together
        result_as_map = rdd.collectAsMap()
    finally:
        sc.stop()
    return result_as_map


def match_satellite_to_insitu(tile_ids, primary_b, matchup_b, parameter_b, tt_b, dt_b, rt_b, platforms_b,
                              bounding_wkt_b):
    from itertools import chain
    from datetime import datetime
    from pytz import UTC
    from webservice.algorithms_spark.Matchup import DomsPoint  # Must import DomsPoint or Spark complains

    tile_service = NexusTileService()
    edge_session = requests.Session()

    match_generators = []
    with edge_session:
        for tile_id in tile_ids:
            # Load tile
            try:
                the_time = datetime.now()
                tile = tile_service.mask_tiles_to_polygon(wkt.loads(bounding_wkt_b.value),
                                                          tile_service.find_tile_by_id(tile_id))[0]
                print "%s Time to load tile %s" % (str(datetime.now() - the_time), tile_id)
            except IndexError:
                # This should only happen if all measurements in a tile become masked after applying the bounding polygon
                continue

            # Get tile bounding lat/lon and time, expand lat/lon and time by tolerance
            # TODO rt_b is in meters. Need to figure out how to increase lat/lon bbox by meters
            easting, northing, zone_number, zone_letter = utm.from_latlon(tile.bbox.min_lat, tile.bbox.min_lon)
            matchup_min_lon = tile.bbox.min_lon  # - rt_b.value
            matchup_min_lat = tile.bbox.min_lat  # - rt_b.value
            matchup_max_lon = tile.bbox.max_lon  # + rt_b.value
            matchup_max_lat = tile.bbox.max_lat  # + rt_b.value

            matchup_min_time = (tile.min_time - datetime.utcfromtimestamp(0).replace(
                tzinfo=UTC)).total_seconds() - tt_b.value
            matchup_max_time = (tile.max_time - datetime.utcfromtimestamp(0).replace(
                tzinfo=UTC)).total_seconds() + tt_b.value

            # Query edge for points
            the_time = datetime.now()
            edge_results = []
            for insitudata_name in matchup_b.value.split(','):
                bbox = ','.join(
                    [str(matchup_min_lon), str(matchup_min_lat), str(matchup_max_lon), str(matchup_max_lat)])
                edge_response = query_edge(insitudata_name, matchup_min_time, matchup_max_time, bbox, dt_b.value,
                                           platforms_b.value, session=edge_session)
                if edge_response['totalResults'] == 0:
                    continue
                r = edge_response['results']
                for p in r:
                    p['source'] = insitudata_name
                edge_results.extend(r)
            print "%s Time to call edge for tile %s" % (str(datetime.now() - the_time), tile_id)
            if len(edge_results) == 0:
                continue

            # Convert tile measurements to DomsPoints
            the_time = datetime.now()
            nexus_doms_points = [DomsPoint.from_nexus_point(value, tile=tile, parameter=parameter_b.value)
                                 for
                                 value in tile.nexus_point_generator()]
            print "%s Time to convert primary points for tile %s" % (str(datetime.now() - the_time), tile_id)

            # Convert edge points to DomsPoints
            the_time = datetime.now()
            edge_doms_points = [DomsPoint.from_edge_point(value) for value in edge_results]
            print "%s Time to convert match points for tile %s" % (str(datetime.now() - the_time), tile_id)

            # call match_points
            match_generators.append(match_points_generator(nexus_doms_points, edge_doms_points, rt_b.value))

    return chain(*match_generators)


def query_edge(dataset, startTime, endTime, bbox, maxDepth, platform, itemsPerPage=1000, startIndex=0, stats=True,
               session=None):
    import json
    from webservice.algorithms.doms import config as edge_endpoints

    try:
        startTime = datetime.utcfromtimestamp(startTime).strftime('%Y-%m-%dT%H:%M:%SZ')
    except TypeError:
        # Assume we were passed a properly formatted string
        pass

    try:
        endTime = datetime.utcfromtimestamp(endTime).strftime('%Y-%m-%dT%H:%M:%SZ')
    except TypeError:
        # Assume we were passed a properly formatted string
        pass

    try:
        platform = platform.split(',')
    except AttributeError:
        # Assume we were passed a list
        pass

    params = {"startTime": startTime,
              "endTime": endTime,
              "bbox": bbox,
              "maxDepth": maxDepth,
              "platform": platform,
              "itemsPerPage": itemsPerPage, "startIndex": startIndex, "stats": str(stats).lower()}

    if session is not None:
        edge_request = session.get(edge_endpoints.getEndpointByName(dataset)['url'], params=params)
    else:
        edge_request = requests.get(edge_endpoints.getEndpointByName(dataset)['url'], params=params)

    edge_request.raise_for_status()
    edge_response = json.loads(edge_request.text)

    return edge_response


# primary_points and matchup_points are both a list of DomsPoint
# Result is a map of DomsPoint to list of DomsPoint
#   map keys == primary_points
#   map values == subset of matchup_points that match the primary_point key
def match_points_generator(primary_points, matchup_points, r_tol):
    import logging
    log = logging.getLogger(__name__)
    log.debug("Building primary tree")
    the_time = datetime.now()
    p_coords = np.array([(point.sh_point.coords.xy[0][0], point.sh_point.coords.xy[1][0]) for point in primary_points],
                        order='c')
    p_tree = spatial.cKDTree(p_coords, leafsize=30)
    print "%s Time to build primary tree" % (str(datetime.now() - the_time))

    log.debug("Building matchup tree")
    the_time = datetime.now()
    m_coords = np.array([(point.sh_point.coords.xy[0][0], point.sh_point.coords.xy[1][0]) for point in matchup_points],
                        order='c')
    m_tree = spatial.cKDTree(m_coords, leafsize=30)
    print "%s Time to build matchup tree" % (str(datetime.now() - the_time))

    log.debug("Querying primary tree for all nearest neighbors")
    the_time = datetime.now()
    matched_indexes = p_tree.query_ball_tree(m_tree, r_tol)
    print "%s Time to query primary tree" % (str(datetime.now() - the_time))

    log.debug("Building match results")
    for i, point_matches in enumerate(matched_indexes):
        for m_point_index in point_matches:
            yield primary_points[i], matchup_points[m_point_index]


# primary_points and matchup_points are both a list of DomsPoint
# Result is a map of DomsPoint to list of DomsPoint
#   map keys == primary_points
#   map values == subset of matchup_points that match the primary_point key
# TODO Not used in favor of match_points_generator. I'm keeping it around to test the performance vs. match_points_generator
def match_points(primary_points, matchup_points, radius_tolerance):
    import logging
    log = logging.getLogger(__name__)
    log.debug("Building primary tree")
    p_coords = np.array([(point.sh_point.coords.xy[0][0], point.sh_point.coords.xy[1][0]) for point in primary_points],
                        order='c')
    p_tree = spatial.cKDTree(p_coords, leafsize=30)

    log.debug("Building matchup tree")
    m_coords = np.array([(point.sh_point.coords.xy[0][0], point.sh_point.coords.xy[1][0]) for point in matchup_points],
                        order='c')
    m_tree = spatial.cKDTree(m_coords, leafsize=30)

    log.debug("Querying primary tree for all nearest neighbors")
    matched_indexes = p_tree.query_ball_tree(m_tree, radius_tolerance)

    log.debug("Building match results")
    matched_points = {primary_point: [matchup_points[m_point_index] for m_point_index in matched_indexes[i] if
                                      m_point_index < len(matchup_points)] for
                      i, primary_point in enumerate(primary_points)}

    return matched_points
