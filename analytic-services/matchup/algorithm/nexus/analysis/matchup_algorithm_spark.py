"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import requests
import utm
from shapely.geometry import Point
from datetime import datetime


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

        point.wind_u = edge_point.get('eastward_wind')
        point.wind_v = edge_point.get('northward_wind')
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
    with sc:
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

    return result_as_map


def match_satellite_to_insitu(tile_ids, primary_b, matchup_b, parameter_b, tt_b, dt_b, rt_b, platforms_b,
                              bounding_wkt_b):
    from itertools import chain
    from datetime import datetime
    from shapely.geos import ReadingError
    from shapely import wkt
    from scipy import spatial
    from nexus.data_access import NexusTileService

    tile_ids = list(tile_ids)
    if len(tile_ids) == 0:
        return []
    tile_service = NexusTileService()

    # Query edge for all points in the current partition of tiles
    tiles_bbox = tile_service.get_bounding_box(tile_ids)
    tiles_min_time = tile_service.get_min_time(tile_ids)
    tiles_max_time = tile_service.get_max_time(tile_ids)

    # TODO rt_b is in meters. Need to figure out how to increase lat/lon bbox by meters
    matchup_min_lon = tiles_bbox.bounds[0]  # - rt_b.value
    matchup_min_lat = tiles_bbox.bounds[1]  # - rt_b.value
    matchup_max_lon = tiles_bbox.bounds[2]  # + rt_b.value
    matchup_max_lat = tiles_bbox.bounds[3]  # + rt_b.value

    # TODO in situ points are no longer guaranteed to be within the tolerance window for an individual tile. Add filter?
    matchup_min_time = tiles_min_time - tt_b.value
    matchup_max_time = tiles_max_time + tt_b.value

    # Query edge for points
    the_time = datetime.now()
    edge_session = requests.Session()
    edge_results = []
    with edge_session:
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
    print "%s Time to call edge for partition %s to %s" % (str(datetime.now() - the_time), tile_ids[0], tile_ids[-1])
    if len(edge_results) == 0:
        return []

    # Convert edge points to utm
    the_time = datetime.now()
    matchup_points = []
    for edge_point in edge_results:
        try:
            x, y = wkt.loads(edge_point['point']).coords[0]
        except ReadingError:
            try:
                x, y = Point(*[float(c) for c in edge_point['point'].split(' ')]).coords[0]
            except ValueError:
                y, x = Point(*[float(c) for c in edge_point['point'].split(',')]).coords[0]

        matchup_points.append(utm.from_latlon(y, x)[0:2])
    print "%s Time to convert match points for partition %s to %s" % (
        str(datetime.now() - the_time), tile_ids[0], tile_ids[-1])

    # Build kdtree from matchup points
    the_time = datetime.now()
    m_tree = spatial.cKDTree(matchup_points, leafsize=30)
    print "%s Time to build matchup tree" % (str(datetime.now() - the_time))

    # The actual matching happens in the generator. This is so that we only load 1 tile into memory at a time
    match_generators = [match_tile_to_point_generator(tile_service, tile_id, m_tree, edge_results, bounding_wkt_b.value,
                                                      parameter_b.value, rt_b.value) for tile_id in tile_ids]

    return chain(*match_generators)


def match_tile_to_point_generator(tile_service, tile_id, m_tree, edge_results, search_domain_bounding_wkt,
                                  search_parameter, radius_tolerance):
    import numpy as np
    from scipy import spatial
    from shapely import wkt
    from nexus.data_access.model import NexusPoint
    from nexus.analysis.matchup_algorithm_spark import DomsPoint  # Must import DomsPoint or Spark complains

    # Load tile
    try:
        the_time = datetime.now()
        tile = tile_service.mask_tiles_to_polygon(wkt.loads(search_domain_bounding_wkt),
                                                  tile_service.find_tile_by_id(tile_id))[0]
        print "%s Time to load tile %s" % (str(datetime.now() - the_time), tile_id)
    except IndexError:
        # This should only happen if all measurements in a tile become masked after applying the bounding polygon
        raise StopIteration

    # Convert valid tile lat,lon tuples to UTM tuples
    the_time = datetime.now()
    # Get list of indices of valid values
    valid_indices = tile.get_indices()
    primary_points = np.array([utm.from_latlon(tile.latitudes[aslice[1]], tile.longitudes[aslice[2]])[0:2] for
                               aslice in valid_indices])
    print "%s Time to convert primary points for tile %s" % (str(datetime.now() - the_time), tile_id)

    a_time = datetime.now()
    p_tree = spatial.cKDTree(primary_points, leafsize=30)
    print "%s Time to build primary tree" % (str(datetime.now() - a_time))

    a_time = datetime.now()
    matched_indexes = p_tree.query_ball_tree(m_tree, radius_tolerance)
    print "%s Time to query primary tree for tile %s" % (str(datetime.now() - a_time), tile_id)
    for i, point_matches in enumerate(matched_indexes):
        if len(point_matches) > 0:
            p_nexus_point = NexusPoint(tile.latitudes[valid_indices[i][1]],
                                       tile.longitudes[valid_indices[i][2]], None,
                                       tile.times[valid_indices[i][0]], valid_indices[i],
                                       tile.data[tuple(valid_indices[i])])
            p_doms_point = DomsPoint.from_nexus_point(p_nexus_point, tile=tile, parameter=search_parameter)
            for m_point_index in point_matches:
                m_doms_point = DomsPoint.from_edge_point(edge_results[m_point_index])
                yield p_doms_point, m_doms_point


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
