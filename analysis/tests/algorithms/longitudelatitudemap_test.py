"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import json
import time
import unittest
import urllib
from multiprocessing.pool import ThreadPool
from unittest import skip

from mock import MagicMock
from nexustiles.nexustiles import NexusTileService
from shapely.geometry import box
from tornado.testing import AsyncHTTPTestCase, bind_unused_port
from tornado.web import Application

from NexusHandler import AlgorithmModuleWrapper
from webapp import ModularNexusHandlerWrapper
from webmodel import NexusRequestObject
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


class HttpIntegrationTest(unittest.TestCase):
    def get_app(self):
        path = LongitudeLatitudeMap.LongitudeLatitudeMapHandlerImpl.path
        algorithm = AlgorithmModuleWrapper(LongitudeLatitudeMap.LongitudeLatitudeMapHandlerImpl)
        thread_pool = ThreadPool(processes=1)
        return Application(
            [(path, ModularNexusHandlerWrapper, dict(clazz=algorithm, algorithm_config=None, thread_pool=thread_pool))],
            default_host=bind_unused_port()
        )

    # @skip("Integration test only. Works only if you have Solr and Cassandra running locally with data ingested")
    def test_integration_all_in_tile(self):
        def get_argument(*args, **kwargs):
            params = {
                "ds": "MXLDEPTH_ECCO_version4_release1",
                "minLon": "-45",
                "minLat": "0",
                "maxLon": "0",
                "maxLat": "45",
                "startTime": "1992-01-01T00:00:00Z",
                "endTime": "2016-12-01T00:00:00Z"
            }
            return params[args[0]]
        request_handler_mock = MagicMock()
        request_handler_mock.get_argument.side_effect = get_argument
        request = NexusRequestObject(request_handler_mock)
        handler_impl = LongitudeLatitudeMap.LongitudeLatitudeMapHandlerImpl()

        response = handler_impl.calc(request)

        print response.toJson()
