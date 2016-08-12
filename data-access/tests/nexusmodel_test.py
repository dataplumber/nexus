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
