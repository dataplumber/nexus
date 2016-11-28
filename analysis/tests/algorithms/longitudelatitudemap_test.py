"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import time
import unittest

from nexustiles.nexustiles import NexusTileService
from shapely.geometry import box

from webservice.algorithms import LongitudeLatitudeMap


class TestLongitudeLatitudeMap(unittest.TestCase):
    def setUp(self):
        self.tile_service = NexusTileService()

    def test_lin_reg(self):
        LongitudeLatitudeMap.tile_service = self.tile_service
        print next(
            LongitudeLatitudeMap.regression_on_tiles((175.01, -42.68, 180.0, -40.2), box(-180, -90, 180, 90).wkt, 1,
                                                     time.time(), "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"))

    def test_lat_lon_map_driver(self):
        # LongitudeLatitudeMap.tile_service = self.tile_service
        print next(iter(LongitudeLatitudeMap.lat_lon_map_driver(box(-180, -90, 180, 90), 1, time.time(),
                                                      "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1",
                                                      [(175.01, -42.68, 180.0, -40.2)])))
