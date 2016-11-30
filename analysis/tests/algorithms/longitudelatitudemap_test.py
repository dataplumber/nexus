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

    def test_lat_lon_map_driver_mur(self):
        # LongitudeLatitudeMap.tile_service = self.tile_service
        print next(iter(LongitudeLatitudeMap.lat_lon_map_driver(box(-180, -90, 180, 90), 1, time.time(),
                                                                "JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1",
                                                                [(175.01, -42.68, 180.0, -40.2)])))

    def test_lat_lon_map_driver_ecco(self):
        bounding = box(-148, 38, -129, 53)
        ds = "MXLDEPTH_ECCO_version4_release1"
        start_seconds_from_epoch = 1
        end_seconds_from_epoch = time.time()
        boxes = self.tile_service.get_distinct_bounding_boxes_in_polygon(bounding, ds,
                                                                         start_seconds_from_epoch,
                                                                         end_seconds_from_epoch)
        print LongitudeLatitudeMap.LongitudeLatitudeMapHandlerImpl.results_to_dicts(
            LongitudeLatitudeMap.lat_lon_map_driver(bounding, start_seconds_from_epoch, end_seconds_from_epoch, ds,
                                                    [a_box.bounds for a_box in boxes]))
