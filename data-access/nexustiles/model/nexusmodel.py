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

        try:
            summary['latitudes'] = self.latitudes.shape
        except AttributeError:
            summary['latitudes'] = 'None'

        try:
            summary['longitudes'] = self.longitudes.shape
        except AttributeError:
            summary['longitudes'] = 'None'

        try:
            summary['times'] = self.times.shape
        except AttributeError:
            summary['times'] = 'None'

        try:
            summary['data'] = self.data.shape
        except AttributeError:
            summary['data'] = 'None'

        try:
            summary['meta_data'] = {meta_name: meta_array.shape for meta_name, meta_array in self.meta_data.iteritems()}
        except AttributeError:
            summary['meta_data'] = 'None'

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
            for index in np.transpose(np.ma.nonzero(self.data)):
                index = tuple(index)
                time = self.times[index[0]]
                lat = self.latitudes[index[1]]
                lon = self.longitudes[index[2]]
                data_val = self.data[index]
                point = NexusPoint(lat, lon, None, time, index, data_val)
                yield point

    def get_indicies(self, include_nan=False):
        if include_nan:
            return list(np.ndindex(self.data.shape))
        else:
            return np.transpose(np.ma.nonzero(self.data)).tolist()

    def contains_point(self, lat, lon):
        return (
                   (self.bbox.min_lat < lat or np.isclose(self.bbox.min_lat, lat)) and
                   (lat < self.bbox.max_lat or np.isclose(lat, self.bbox.max_lat))
               ) and (
                   (self.bbox.min_lon < lon or np.isclose(self.bbox.min_lon, lon)) and
                   (lon < self.bbox.max_lon or np.isclose(lon, self.bbox.max_lon))
               )


def get_approximate_value_for_lat_lon(tile_list, lat, lon, arrayName=None):
    """
    This function pulls the value out of one of the tiles in tile_list that is the closest to the given
    lat, lon point.

    :returns float value closest to lat lon point or float('Nan') if the point is masked or not contained in any tile
    """

    try:
        tile = next(tile for tile in tile_list if tile.contains_point(lat, lon))
    except StopIteration:
        # lat or lon are out of bounds for these tiles, return nan
        return float('NaN')

    # Check latitude array for an exact match on the latitude being searched for
    lat_idx = np.ma.where(np.isclose(tile.latitudes, lat))[0]
    if len(lat_idx) == 0:
        # No exact match but the lat being search for is between min and max. So break the lats into all values less
        # than the lat being searched for, then take the last one in that list
        lat_idx = next(iter(np.ma.where((lat > tile.latitudes))))[-1]
    else:
        lat_idx = lat_idx[0]

    # Repeat for longitude
    lon_idx = np.ma.where(np.isclose(tile.longitudes, lon))[0]
    if len(lon_idx) == 0:
        lon_idx = next(iter(np.ma.where((lon > tile.longitudes))))[-1]
    else:
        lon_idx = lon_idx[0]

    try:
        if arrayName is None or arrayName == "data":
            data_val = tile.data[0][lat_idx][lon_idx]
        else:
            data_val = tile.meta_data[arrayName][lat_idx][lon_idx]
    except IndexError:
        return None

    return data_val.item() if (data_val is not np.ma.masked) and data_val.size == 1 else float('Nan')
