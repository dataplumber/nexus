"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import unittest
import numpy as np
from nexustiles.model.nexusmodel import get_approximate_value_for_lat_lon, Tile, BBox


class TestApproximateValueMethod(unittest.TestCase):
    def test_lat_exact_lon_exact(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(22.0, get_approximate_value_for_lat_lon([tile], 1.0, 0))

    def test_lat_lon_exact_min(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(0, get_approximate_value_for_lat_lon([tile], -1.0, -2.0))

    def test_lat_approx_lon_exact(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(5.0, get_approximate_value_for_lat_lon([tile], -0.4, -2.0))

    def test_lat_approx_lon_approx(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(7.0, get_approximate_value_for_lat_lon([tile], -0.4, 0.01))

    def test_lat_exact_lon_approx(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(18.0, get_approximate_value_for_lat_lon([tile], 0.5, 1.01))

    def test_lat_greater_than_bounds(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertTrue(np.isnan(get_approximate_value_for_lat_lon([tile], 2.0, 0)))

    def test_lat_less_than_bounds(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertTrue(np.isnan(get_approximate_value_for_lat_lon([tile], -2.0, 0)))

    def test_lon_greater_than_bounds(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertTrue(np.isnan(get_approximate_value_for_lat_lon([tile], 0, 3)))

    def test_lon_less_than_bounds(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        #  0.    [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertTrue(np.isnan(get_approximate_value_for_lat_lon([tile], 0, -3)))

    def test_repeated_lats(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, -0.5, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))

        #         -2   -1   0    1    2
        # -1.0 [[[0.   1.   2.   3.   4. ]
        # -0.5   [5.   6.   7.   8.   9. ]
        # -0.5   [10.  11.  12.  13.  14.]
        #  0.5   [15.  16.  17.  18.  19.]
        #  1.0   [20.  21.  22.  23.  24.]]]

        self.assertAlmostEqual(11, get_approximate_value_for_lat_lon([tile], -0.4, -1))


class TestTileContainsMethod(unittest.TestCase):

    def test_masked_tile(self):
        tile = Tile()
        tile.bbox = BBox(30.5, 37.5, -51.5, -36.5)
        tile.latitudes = np.ma.arange(30.5, 38.5, 1.0)
        tile.longitudes = np.ma.arange(-51.5, -35.5, 1.0)
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(128.0).reshape((1, 8, 16))

        # tile.latitudes [ 30.5  31.5  32.5  33.5  34.5  35.5  36.5  37.5]
        tile.latitudes = np.ma.masked_outside(tile.latitudes, 35, 45)
        # tile.latitudes [-- -- -- -- -- 35.5 36.5 37.5]

        # tile.longitudes [-51.5 -50.5 -49.5 -48.5 -47.5 -46.5 -45.5 -44.5 -43.5 -42.5 -41.5 -40.5 -39.5  -38.5 -37.5 -36.5]
        tile.longitudes = np.ma.masked_outside(tile.longitudes, -50, -40)
        # tile.longitudes [-- -- -49.5 -48.5 -47.5 -46.5 -45.5 -44.5 -43.5 -42.5 -41.5 -40.5 -- -- -- --]

        # Tile no longer contains 35, -50
        self.assertFalse(tile.contains_point(35, -50))


