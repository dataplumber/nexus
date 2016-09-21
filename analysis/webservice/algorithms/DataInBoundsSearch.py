"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
# distutils: include_dirs = /usr/local/lib/python2.7/site-packages/cassandra
import pyximport

pyximport.install()

from webservice.NexusHandler import NexusHandler, nexus_handler, DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults


@nexus_handler
class DataInBoundsSearchHandlerImpl(NexusHandler):
    name = "Data In-Bounds Search"
    path = "/datainbounds"
    description = "Fetches point values for a given dataset and geographical area"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        NexusHandler.__init__(self)

    def calc(self, computeOptions, **args):
        min_lat = computeOptions.get_min_lat()
        max_lat = computeOptions.get_max_lat()
        min_lon = computeOptions.get_min_lon()
        max_lon = computeOptions.get_max_lon()
        ds = computeOptions.get_dataset()[0]
        start_time = computeOptions.get_start_time()
        end_time = computeOptions.get_end_time()
        includemeta = computeOptions.get_include_meta()

        tiles = self._tile_service.get_tiles_bounded_by_box(min_lat, max_lat, min_lon, max_lon, ds, start_time,
                                                            end_time)

        data = []
        for tile in tiles:
            for nexus_point in tile.nexus_point_generator():
                data.append({
                    'latitude': nexus_point.latitude,
                    'longitude': nexus_point.longitude,
                    'time': nexus_point.time,
                    'data': [
                        {
                            'id': tile.tile_id,
                            'value': nexus_point.data_val
                        }
                    ]
                })

        if includemeta and len(tiles) > 0:
            meta = [tile.get_summary() for tile in tiles]
        else:
            meta = None

        result = NexusResults(
            results=data,
            stats={},
            meta=meta)

        result.extendMeta(min_lat, max_lat, min_lon, max_lon, "", start_time, end_time)

        return result
