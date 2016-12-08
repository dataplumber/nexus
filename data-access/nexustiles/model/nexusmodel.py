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
        return str(self.get_summary())

    def get_summary(self):
        summary = dict(self.__dict__)

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

    def get_indices(self, include_nan=False):
        if include_nan:
            return list(np.ndindex(self.data.shape))
        else:
            return np.transpose(np.where(np.ma.getmaskarray(self.data) == False)).tolist()

    def contains_point(self, lat, lon):

        return contains_point(self.latitudes, self.longitudes, lat, lon)


def contains_point(latitudes, longitudes, lat, lon):
    minx, miny, maxx, maxy = np.ma.min(longitudes), np.ma.min(latitudes), np.ma.max(
        longitudes), np.ma.max(latitudes)
    return (
               (miny < lat or np.isclose(miny, lat)) and
               (lat < maxy or np.isclose(lat, maxy))
           ) and (
               (minx < lon or np.isclose(minx, lon)) and
               (lon < maxx or np.isclose(lon, maxx))
           )


def merge_tiles(tile_list):
    a = np.array([tile.times for tile in tile_list])
    assert np.ma.max(a) == np.ma.min(a)

    merged_times = tile_list[0].times
    merged_lats = np.ndarray((0,))
    merged_lons = np.ndarray((0,))
    merged_data = np.ndarray((0, 0))

    for tile in tile_list:
        if tile.latitudes in merged_lats and tile.longitudes not in merged_lons:
            merged_lons = np.ma.concatenate([merged_lons, tile.longitudes])
            merged_data = np.ma.hstack((merged_data, np.ma.squeeze(tile.data)))
        elif tile.latitudes not in merged_lats and tile.longitudes in merged_lons:
            merged_lats = np.ma.concatenate([merged_lats, tile.latitudes])
            merged_data = np.ma.vstack((merged_data, np.ma.squeeze(tile.data)))
        elif tile.latitudes not in merged_lats and tile.longitudes not in merged_lons:
            merged_lats = np.ma.concatenate([merged_lats, tile.latitudes])
            merged_lons = np.ma.concatenate([merged_lons, tile.longitudes])
            merged_data = block_diag(*[merged_data, np.ma.squeeze(tile.data)])
        else:
            raise Exception("Can't handle overlapping tiles")

    merged_data = merged_data[np.ma.argsort(merged_lats), :]
    merged_data = merged_data[:, np.ma.argsort(merged_lons)]
    merged_lats = merged_lats[np.ma.argsort(merged_lats),]
    merged_lons = merged_lons[np.ma.argsort(merged_lons),]

    merged_data = merged_data[np.newaxis, :]

    return merged_times, merged_lats, merged_lons, merged_data


def block_diag(*arrs):
    """Create a block diagonal matrix from the provided arrays.

    Given the inputs `A`, `B` and `C`, the output will have these
    arrays arranged on the diagonal::

        [[A, 0, 0],
         [0, B, 0],
         [0, 0, C]]

    If all the input arrays are square, the output is known as a
    block diagonal matrix.

    Parameters
    ----------
    A, B, C, ... : array-like, up to 2D
        Input arrays.  A 1D array or array-like sequence with length n is
        treated as a 2D array with shape (1,n).

    Returns
    -------
    D : ndarray
        Array with `A`, `B`, `C`, ... on the diagonal.  `D` has the
        same dtype as `A`.

    References
    ----------
    .. [1] Wikipedia, "Block matrix",
           http://en.wikipedia.org/wiki/Block_diagonal_matrix

    Examples
    --------
    >>> A = [[1, 0],
    ...      [0, 1]]
    >>> B = [[3, 4, 5],
    ...      [6, 7, 8]]
    >>> C = [[7]]
    >>> print(block_diag(A, B, C))
    [[1 0 0 0 0 0]
     [0 1 0 0 0 0]
     [0 0 3 4 5 0]
     [0 0 6 7 8 0]
     [0 0 0 0 0 7]]
    >>> block_diag(1.0, [2, 3], [[4, 5], [6, 7]])
    array([[ 1.,  0.,  0.,  0.,  0.],
           [ 0.,  2.,  3.,  0.,  0.],
           [ 0.,  0.,  0.,  4.,  5.],
           [ 0.,  0.,  0.,  6.,  7.]])

    """
    if arrs == ():
        arrs = ([],)
    arrs = [np.atleast_2d(a) for a in arrs]

    bad_args = [k for k in range(len(arrs)) if arrs[k].ndim > 2]
    if bad_args:
        raise ValueError("arguments in the following positions have dimension "
                         "greater than 2: %s" % bad_args)

    shapes = np.array([a.shape for a in arrs])
    out = np.ma.masked_all(np.sum(shapes, axis=0), dtype=arrs[0].dtype)

    r, c = 0, 0
    for i, (rr, cc) in enumerate(shapes):
        out[r:r + rr, c:c + cc] = arrs[i]
        r += rr
        c += cc
    return out


def find_nearest(array, value):
    idx = (np.abs(array - value)).argmin()
    return array[idx]


def get_approximate_value_for_lat_lon(tile_list, lat, lon):
    """
    This function pulls the value out of one of the tiles in tile_list that is the closest to the given
    lat, lon point.

    :returns float value closest to lat lon point or float('Nan') if the point is masked or not contained in any tile
    """

    try:
        times, lats, longs, data = merge_tiles(tile_list)
        if not contains_point(lats, longs, lat, lon):
            # Lat, Lon is out of bounds for these tiles
            return float('NaN')
    except AssertionError:
        # Tiles are not all at the same time
        return float('NaN')

    nearest_lat = find_nearest(lats, lat)
    nearest_long = find_nearest(longs, lon)

    data_val = data[0][(np.abs(lats - lat)).argmin()][(np.abs(longs - lon)).argmin()]

    return data_val.item() if (data_val is not np.ma.masked) and data_val.size == 1 else float('Nan')
