"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from math import fabs

import numpy as np
import requests
import utm
from datetime import datetime
from nexustiles.nexustiles import NexusTileService
from pyspark import SparkContext, SparkConf
from scipy import spatial
from shapely import wkt
from shapely.geometry import Point
from webservice.NexusHandler import NexusHandler, nexus_handler


# TODO Need to handle matchup of different parameters
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
            "type": "long",
            "description": "Starting time in milliseconds since midnight Jan. 1st, 1970 UTC"
        },
        "endTime": {
            "name": "End Time",
            "type": "long",
            "description": "Ending time in milliseconds since midnight Jan. 1st, 1970 UTC"
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

    def calc(self, request, **args):
        # TODO Assuming Satellite primary
        bounding_polygon = request.get_bounding_polygon()
        primary_ds_name = request.get_argument('primary', None)
        matchup_ds_names = request.get_argument('matchup', None)
        parameter_s = request.get_argument('parameter', 'sst')
        start_time = request.get_start_time()
        end_time = request.get_end_time()
        time_tolerance = request.get_int_arg('tt', default=0)
        depth_tolerance = request.get_decimal_arg('dt', default=0.0)
        radius_tolerance = request.get_decimal_arg('rt', default=0)
        platforms = request.get_argument('platforms')

        # Get tile ids in box
        tile_ids = self._tile_service.find_tiles_in_polygon(bounding_polygon, primary_ds_name, start_time, end_time,
                                                            fetch_data=False)

        # Call spark_matchup
        spark_matchup_driver(tile_ids, wkt.dumps(bounding_polygon), primary_ds_name, matchup_ds_names, parameter_s,
                             time_tolerance, depth_tolerance, radius_tolerance, platforms)

        pass


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

    def is_close_to(self, other_point, xy_tolerance, depth_tolerance):

        is_depth_close = True

        if self.depth is not None and other_point.depth is not None and fabs(
                        self.depth - other_point.depth) > depth_tolerance:
            is_depth_close = False

        primary_point = self.sh_point if xy_tolerance <= 0 else self.sh_point.buffer(xy_tolerance)

        return is_depth_close and primary_point.contains(other_point.sh_point)

    @staticmethod
    def from_nexus_point(nexus_point, tile=None, data_id=None, parameter='sst'):
        point = DomsPoint()

        if data_id is not None:
            point.data_id = data_id
        else:
            point.data_id = hash(nexus_point)

        # TODO Not an ideal solution; but it works for now.
        if parameter == 'sst':
            point.sst = nexus_point.data_val
        elif parameter == 'sss':
            point.sss = nexus_point.data_val
        elif parameter == 'wind':
            point.wind_u = nexus_point.data_val
            try:
                point.wind_v = tile.meta_data['wind_v'][nexus_point.index]
            except (KeyError, IndexError):
                pass
            try:
                point.wind_direction = tile.meta_data['wind_direction'][nexus_point.index]
            except (KeyError, IndexError):
                pass
            try:
                point.wind_speed = tile.meta_data['wind_speed'][nexus_point.index]
            except (KeyError, IndexError):
                pass
        else:
            raise NotImplementedError('%s not supported. Only sst, sss, and wind parameters are supported.' % parameter)

        point.longitude = nexus_point.longitude
        point.latitude = nexus_point.latitude
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
        point.platform = 9
        point.device = 5
        return point

    @staticmethod
    def from_edge_point(edge_point, data_id=None):
        point = DomsPoint()

        if data_id is not None:
            point.data_id = data_id
        else:
            point.data_id = hash(edge_point)

        x, y = map(lambda coord: float(coord), edge_point['point'].split(' '))
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
        return point


def spark_matchup_driver(tile_ids, bounding_wkt, primary_ds_name, matchup_ds_names, parameter, time_tolerance,
                         depth_tolerance, radius_tolerance, platforms):
    from functools import partial

    # Configure Spark
    sp_conf = SparkConf()
    sp_conf.setAppName("Spark Matchup")

    sp_conf.setMaster("local[1]")
    sc = SparkContext(conf=sp_conf)

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
        partial(match_satellite_to_insitu, primary_b=primary_b, matchup_b=matchup_b, parameter_b=parameter_b, tt_b=tt_b,
                dt_b=dt_b, rt_b=rt_b, platforms_b=platforms_b, bounding_wkt_b=bounding_wkt_b),
        preservesPartitioning=True) \
        .combineByKey(lambda value: [value],  # Create 1 element list
                      lambda value_list, value: value_list + [value],  # Add 1 element to list
                      lambda value_list_a, value_list_b: value_list_a + value_list_b)  # Add two lists together
    return rdd.collectAsMap()


def match_satellite_to_insitu(tile_ids, primary_b, matchup_b, parameter_b, tt_b, dt_b, rt_b, platforms_b,
                              bounding_wkt_b):
    from itertools import chain
    from datetime import datetime
    from pytz import UTC
    from Matchup import DomsPoint  # Must import DomsPoint or Spark complains
    import json

    tile_service = NexusTileService()
    edge_session = requests.Session()

    match_generators = []
    for tile_id in tile_ids:
        # Load tile
        try:
            tile = tile_service.mask_tiles_to_polygon(wkt.loads(bounding_wkt_b.value),
                                                      tile_service.find_tile_by_id(tile_id))[0]
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
        # TODO Encapsulate edge call? At minimum, pull config values out
        params = {"startTime": datetime.utcfromtimestamp(matchup_min_time).strftime('%Y-%m-%dT%H:%M:%SZ'),
                  "endTime": datetime.utcfromtimestamp(matchup_max_time).strftime('%Y-%m-%dT%H:%M:%SZ'),
                  "bbox": ','.join(
                      [str(matchup_min_lon), str(matchup_min_lat), str(matchup_max_lon), str(matchup_max_lat)]),
                  "itemsPerPage": 1000, "startIndex": 0,
                  "maxDepth": dt_b.value, "stats": "true",
                  "platform": platforms_b.value.split(',')}
        edge_request = edge_session.get("http://127.0.0.1:8890/ws/search/spurs", params=params)
        edge_request.raise_for_status()
        edge_results = json.loads(edge_request.text)['results']

        # Convert tile measurements to DomsPoints
        nexus_points = {hash(p): p for p in tile.nexus_point_generator()}
        nexus_doms_points = [DomsPoint.from_nexus_point(value, data_id=key, tile=tile, parameter=parameter_b.value) for
                             key, value in nexus_points.iteritems()]

        # Convert edge points to DomsPoints
        edge_points = {hash(frozenset(p.items())): p for p in edge_results}
        edge_doms_points = [DomsPoint.from_edge_point(value, data_id=key) for key, value in edge_points.iteritems()]

        # call match_points
        match_generators.append(match_points_generator(nexus_doms_points, edge_doms_points, rt_b.value))

    edge_session.close()

    return chain(*match_generators)


# primary_points and matchup_points are both a list of DomsPoint
# Result is a map of DomsPoint to list of DomsPoint
#   map keys == primary_points
#   map values == subset of matchup_points that match the primary_point key
def match_points_generator(primary_points, matchup_points, r_tol):
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
    matched_indexes = p_tree.query_ball_tree(m_tree, r_tol)

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
