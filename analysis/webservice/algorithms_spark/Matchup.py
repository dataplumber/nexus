"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import logging
from math import fabs

import numpy as np
import utm
import requests
from scipy import spatial
from shapely.geometry import Point
from webservice.NexusHandler import NexusHandler, nexus_handler
from nexustiles.nexustiles import NexusTileService
from pyspark import SparkContext, SparkConf

log = logging.getLogger(__name__)


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
            "name": "Matchup Datasets",
            "type": "comma-delimited string",
            "description": "The Dataset(s) being searched for measurements that match the measurements in Primary"
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

    def get_args(self, request):
        min_lat, max_lat, min_lon, max_lon = request.get_min_lat(), request.get_max_lat(), request.get_min_lon(), request.get_max_lon()
        dataset1 = request.get_argument("ds1", None)
        dataset2 = request.get_argument("ds2", None)
        start_time = request.get_start_time()
        end_time = request.get_end_time()

    def calc(self, request, **args):
        # Assume Satellite primary
        bounding_polygon = request.get_bounding_polygon()
        primary_ds_name = request.get_argument('primary', None)
        matchup_ds_names = request.get_argument('matchup', None)
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
        spark_matchup_driver(tile_ids, primary_ds_name, matchup_ds_names, time_tolerance,
                             depth_tolerance, radius_tolerance, platforms)

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
    def from_nexus_point(nexus_point, source=None):
        point = DomsPoint()

        point.data_id = hash(nexus_point)

        point.longitude = nexus_point.longitude
        point.latitude = nexus_point.latitude
        easting, northing, zone_number, zone_letter = utm.from_latlon(point.latitude, point.longitude)
        point.sh_point = Point(easting, northing)

        point.time = nexus_point.time

        try:
            point.depth = nexus_point.depth
        except KeyError:
            # No depth associated with this measurement
            pass

        point.sst = nexus_point.data_val
        point.sst_depth = 0
        point.source = source
        point.platform = 9
        point.device = 5
        return point

    @staticmethod
    def from_edge_point(edge_point):
        point = DomsPoint()

        point.data_id = hash(edge_point)

        x, y = edge_point['point'].split(' ')

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


def spark_matchup_driver(tile_ids, primary_ds_name, matchup_ds_names, time_tolerance, depth_tolerance, radius_tolerance,
                         platforms):
    from functools import partial
    # Configure Spark
    sp_conf = SparkConf()
    sp_conf.setAppName("Spark Matchup")

    sp_conf.setMaster("local[1]")
    sc = SparkContext(conf=sp_conf)

    # Broadcast parameters
    global primary_b
    global matchup_b
    global tt_b
    global dt_b
    global rt_b
    global platforms_b
    primary_b = sc.broadcast(primary_ds_name)
    matchup_b = sc.broadcast(matchup_ds_names)
    tt_b = sc.broadcast(time_tolerance)
    dt_b = sc.broadcast(depth_tolerance)
    rt_b = sc.broadcast(radius_tolerance)
    platforms_b = sc.broadcast(platforms)

    # Parallelize list of tile ids
    rdd = sc.parallelize(tile_ids)
    # Map Partitions ( list(tile_id) )
    rdd = rdd.mapPartitions(do_map) \
        .groupByKey()
    print rdd.collect()


def do_map(part):
    from itertools import chain
    from datetime import datetime
    from pytz import UTC

    tile_service = NexusTileService()
    edge_session = requests.Session()

    match_generators = []
    for tile_id in tile_ids:
        # Load tile
        tile = tile_service.find_tile_by_id(tile_id)[0]

        # Get tile bounding lat/lon and time, expand lat/lon and time by tolerance
        matchup_min_lon = tile.bbox.min_lon - rt_b.value
        matchup_min_lat = tile.bbox.min_lat - rt_b.value
        matchup_max_lon = tile.bbox.max_lon + rt_b.value
        matchup_max_lat = tile.bbox.max_lat + rt_b.value

        matchup_min_time = (tile.min_time - datetime.utcfromtimestamp(0).replace(tzinfo=UTC)).total_seconds() - tt_b.value
        matchup_max_time = (tile.max_time - datetime.utcfromtimestamp(0).replace(tzinfo=UTC)).total_seconds() + tt_b.value

        # Query edge for points
        params = {"startTime": datetime.utcfromtimestamp(matchup_min_time).strftime('%Y-%m-%dT%H:%M:%SZ'),
                  "endTime": datetime.utcfromtimestamp(matchup_max_time).strftime('%Y-%m-%dT%H:%M:%SZ'),
                  "bbox": ','.join(
                      [str(matchup_min_lon), str(matchup_min_lat), str(matchup_max_lon), str(matchup_max_lat)]),
                  "itemsPerPage": 1000, "startIndex": 0,
                  "maxDepth": dt_b.value, "stats": "true"}
        edge_request = edge_session.get("http://127.0.0.1:8890/ws/search/spurs", params=params)
        edge_request.raise_for_status()
        edge_results = json.loads(edge_request.text)['results']
    #
    #     # Convert tile measurements to DomsPoints
    #     nexus_points = {hash(p): p for p in tile.nexus_point_generator()}
    #     nexus_doms_points = [DomsPoint.from_nexus_point(p, source=tile.dataset) for p in nexus_points.values()]
    #
    #     # Convert edge points to DomsPoints
    #     edge_points = {hash(p): p for p in edge_results}
    #     edge_doms_points = [DomsPoint.from_edge_point(p) for p in edge_points.values()]
    #
    #     # call match_points
    #     match_generators.append(match_points_generator(nexus_doms_points, edge_doms_points, rt_b))

    edge_session.close()

    return chain(match_generators)


def map_matches(tile_ids):
    from itertools import chain

    tile_service = NexusTileService()
    edge_session = requests.Session()

    match_generators = []
    for tile_id in tile_ids:
        # Load tile
        tile = tile_service.find_tile_by_id(tile_id)[0]

        # Get tile bounding lat/lon and time, expand lat/lon and time by tolerance
        matchup_min_lon = tile.bbox.min_lon - rt_b.value
        matchup_min_lat = tile.bbox.min_lat - rt_b.value
        matchup_max_lon = tile.bbox.max_lon + rt_b.value
        matchup_max_lat = tile.bbox.max_lat + rt_b.value

        matchup_min_time = tile.min_time - tt_b
        matchup_max_time = tile.max_time + tt_b

        # Query edge for points
        params = {"startTime": datetime.utcfromtimestamp(matchup_min_time).strftime('%Y-%m-%dT%H:%M:%SZ'),
                  "endTime": datetime.utcfromtimestamp(matchup_max_time).strftime('%Y-%m-%dT%H:%M:%SZ'),
                  "bbox": ','.join(
                      [str(matchup_min_lon), str(matchup_min_lat), str(matchup_max_lon), str(matchup_max_lat)]),
                  "itemsPerPage": 1000, "startIndex": 0,
                  "maxDepth": dt_b, "stats": "true"}
        edge_request = edge_session.get("http://127.0.0.1:8890/ws/search/spurs", params=params)
        edge_request.raise_for_status()
        edge_results = json.loads(edge_request.text)['results']

        # Convert tile measurements to DomsPoints
        nexus_points = {hash(p): p for p in tile.nexus_point_generator()}
        nexus_doms_points = [DomsPoint.from_nexus_point(p, source=tile.dataset) for p in nexus_points.values()]

        # Convert edge points to DomsPoints
        edge_points = {hash(p): p for p in edge_results}
        edge_doms_points = [DomsPoint.from_edge_point(p) for p in edge_points.values()]

        # call match_points
        match_generators.append(match_points_generator(nexus_doms_points, edge_doms_points, rt_b))

    edge_session.close()

    return chain(match_generators)


# primary_points and matchup_points are both a list of DomsPoint
# Result is a map of DomsPoint to list of DomsPoint
#   map keys == primary_points
#   map values == subset of matchup_points that match the primary_point key
def match_points(primary_points, matchup_points, radius_tolerance):
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


def match_points_generator(primary_points, matchup_points, radius_tolerance):
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
    for i, point_matches in enumerate(matched_indexes):
        for m_point_index in point_matches:
            yield (primary_points[i], matchup_points[m_point_index])


if __name__ == "__main__":
    from shapely.wkt import loads
    from datetime import datetime
    import json

    polygon = loads("POLYGON((-34.98 29.54, -30.1 29.54, -30.1 31.00, -34.98 31.00, -34.98 29.54))")
    primary_ds = "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"
    matchup_ds = "spurs"
    start_time = 1350259200  # 2012-10-15T00:00:00Z
    end_time = 1350345600  # 2012-10-16T00:00:00Z
    time_tolerance = 86400
    depth_tolerance = 5.0
    radius_tolerance = 1500
    platforms = "1,2,3,4,5,6,7,8,9"

    tile_service = NexusTileService()
    tile_ids = [tile['id'] for tile in
                tile_service.find_tiles_in_polygon(polygon, primary_ds, start_time, end_time, fetch_data=False,
                                                   fl='id')]
    spark_matchup_driver(tile_ids, primary_ds, matchup_ds, time_tolerance, depth_tolerance, radius_tolerance, platforms)
    # print tile_ids

    # edge_session = requests.Session()
    # params = {"startTime": datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ'),
    #           "endTime": datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ'),
    #           "bbox": ','.join([str(b) for b in polygon.bounds]), "itemsPerPage": 1000, "startIndex": 0,
    #           "maxDepth": depth_tolerance, "stats": "true"}
    # edge_request = edge_session.get("http://127.0.0.1:8890/ws/search/spurs", params=params)
    # edge_request.raise_for_status()
    # print edge_request.text
    # print json.loads(edge_request.text)


def match_points_limit_nearest_neighbor(primary_points, matchup_points, radius_tolerance, num_nearest_neighbor=-1):
    log.debug("Building primary coords")
    p_coords = np.array([(point.sh_point.coords.xy[0][0], point.sh_point.coords.xy[1][0]) for point in primary_points],
                        order='c')
    p_tree = spatial.cKDTree(p_coords, leafsize=30)

    log.debug("Building matchup coords")
    m_coords = np.array([(point.sh_point.coords.xy[0][0], point.sh_point.coords.xy[1][0]) for point in matchup_points],
                        order='c')
    m_tree = spatial.cKDTree(m_coords, leafsize=30)

    if num_nearest_neighbor > 0:
        log.debug("Querying primary tree for %s max nearest neighbors" % num_nearest_neighbor)
        distances, matched_indexes = m_tree.query(p_coords, k=num_nearest_neighbor,
                                                  distance_upper_bound=radius_tolerance)
    else:
        log.debug("Querying primary tree for all nearest neighbors" % num_nearest_neighbor)
        matched_indexes = p_tree.query_ball_tree(m_tree, radius_tolerance)

    log.debug("Building match results")
    matched_points = {primary_point: [matchup_points[m_point_index] for m_point_index in matched_indexes[i] if
                                      m_point_index < len(matchup_points)] for
                      i, primary_point in enumerate(primary_points)}

    return matched_points


def match_points_depth(primary_points, matchup_points, radius_tolerance, depth_tolerance):
    log.debug("Building primary tree")
    p_tree = spatial.cKDTree(
        np.array(
            [(point.sh_point.coords.xy[0][0], point.sh_point.coords.xy[1][0], point.depth) for point in primary_points],
            order='c'), leafsize=30)
    log.debug("Building matchup tree")
    m_tree = spatial.cKDTree(
        np.array(
            [(point.sh_point.coords.xy[0][0], point.sh_point.coords.xy[1][0], point.depth) for point in matchup_points],
            order='c'), leafsize=30)

    log.debug("Querying primary tree")
    matched_indexes = p_tree.query_ball_tree(m_tree, radius_tolerance)

    log.debug("Building match results")
    matched_points = {primary_point: [matchup_points[m_point_index] for m_point_index in matched_indexes[i]] for
                      i, primary_point in enumerate(primary_points)}

    return matched_points


def match_points_carray(primary_points, matchup_points, radius_tolerance, depth_tolerance):
    p_tree = spatial.cKDTree(primary_points, leafsize=30)
    m_tree = spatial.cKDTree(matchup_points, leafsize=30)

    matched_indexes = p_tree.query_ball_tree(m_tree, radius_tolerance)

    return matched_indexes


def match_points_brute(primary_points, matchup_points, radius_tolerance, depth_tolerance):
    matched_points = {primary_point: None for primary_point in primary_points}

    for primary_doms_point in primary_points:
        matches = [matchup_doms_point for matchup_doms_point in matchup_points if
                   primary_doms_point.is_close_to(matchup_doms_point, radius_tolerance, depth_tolerance)]

        matched_points[primary_doms_point] = matches

    return matched_points
