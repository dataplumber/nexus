"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from collections import namedtuple

import numpy as np

NexusPoint = namedtuple('NexusPoint', 'latitude longitude depth time index data_val')
BBox = namedtuple('BBox', 'min_lat max_lat min_lon max_lon')
TileStats = namedtuple('TileStats', 'min max mean count')


class Tile(object):
    def __init__(self):
        self.tile_id = None
        self.dataset_id = None
        self.section_spec = None
        self.dataset = None
        self.granule = None

        self.bbox = None

        self.min_time = None
        self.max_time = None

        self.tile_stats = None

        self.latitudes = None  # This should be a 1-d ndarray
        self.longitudes = None  # This should be a 1-d ndarray
        self.times = None  # This should be a 1-d ndarray
        self.data = None  # This should be an ndarray with shape len(times) x len(latitudes) x len(longitudes)

        self.meta_data = None  # This should be a dict of the form { 'meta_data_name' : [[[ndarray]]] }. Each ndarray should be the same shape as data.

    def __str__(self):
        return self.get_summary()

    def get_summary(self):
        summary = self.__dict__

        summary['latitudes'] = self.latitudes.shape
        summary['longitudes'] = self.longitudes.shape
        summary['times'] = self.times.shape
        summary['data'] = self.data.shape

        summary['meta_data'] = {meta_name: meta_array.shape for meta_name, meta_array in self.meta_data.iteritems()}

        return summary

    def nexus_point_generator(self, include_nan=False):
        if include_nan:
            for index in np.ndindex(self.data.shape):
                time = self.times[index[0]]
                lat = self.latitudes[index[1]]
                lon = self.longitudes[index[2]]
                data_val = self.data[index]
                point = NexusPoint(lat, lon, None, time, index, data_val)
                yield point
        else:
            for index in np.transpose(np.nonzero(self.data)):
                index = tuple(index)
                time = self.times[index[0]]
                lat = self.latitudes[index[1]]
                lon = self.longitudes[index[2]]
                data_val = self.data[index]
                point = NexusPoint(lat, lon, None, time, index, data_val)
                yield point


def get_approximate_value_for_lat_lon(tile_list, lat, lon):
    """
    This function pulls the value out of one of the tiles in tile_list that is the closest to the given
    lat, lon point.

    :returns float value closest to lat lon point or float('Nan') if the point is masked or not contained in any tile
    """

    try:
        tile = next(tile for tile in tile_list if
                    tile.bbox.min_lat <= lat <= tile.bbox.max_lat and tile.bbox.min_lon <= lon <= tile.bbox.max_lon)
    except StopIteration:
        # lat or lon are out of bounds for these tiles, return nan
        return float('NaN')

    lat_idx = np.ma.where(tile.latitudes == lat)
    if len(lat_idx[0]) == 0:
        try:
            lat_idx = next(iter(np.ma.where((lat > tile.latitudes))))[-1]
        except IndexError:
            lat_idx = next(iter(np.ma.where((lat < tile.latitudes))))[0]

    lon_idx = np.ma.where(tile.longitudes == lon)
    if len(lon_idx[0]) == 0:
        try:
            lon_idx = next(iter(np.ma.where((lon > tile.longitudes))))[-1]
        except IndexError:
            lon_idx = next(iter(np.ma.where((lon < tile.longitudes))))[0]

    data_val = tile.data[0][lat_idx][lon_idx]

    return data_val.item() if (data_val is not np.ma.masked) and data_val.size == 1 else float('Nan')
