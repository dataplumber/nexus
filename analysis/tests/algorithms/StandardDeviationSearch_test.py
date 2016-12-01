"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import json
import unittest
import urllib

import numpy as np
from mock import patch, Mock
from nexustiles.model.nexusmodel import Tile, BBox
from nexustiles.nexustiles import NexusTileService
from shapely.geometry import Point
from tornado.testing import AsyncHTTPTestCase, bind_unused_port
from tornado.web import Application
from webservice.algorithms import StandardDeviationSearch
from webservice.webapp import ModularNexusHandlerWrapper
from webservice.NexusHandler import AlgorithmModuleWrapper

from multiprocessing.pool import ThreadPool


class HttpParametersTest(AsyncHTTPTestCase):
    def get_app(self):
        path = StandardDeviationSearch.StandardDeviationSearchHandlerImpl.path
        algorithm = AlgorithmModuleWrapper(StandardDeviationSearch.StandardDeviationSearchHandlerImpl)
        thread_pool = ThreadPool(processes=1)
        return Application(
            [(path, ModularNexusHandlerWrapper, dict(clazz=algorithm, algorithm_config=None, thread_pool=thread_pool))],
            default_host=bind_unused_port()
        )

    def test_no_ds_400(self):
        response = self.fetch(StandardDeviationSearch.StandardDeviationSearchHandlerImpl.path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("'ds' argument is required", body['error'])

    def test_no_longitude_400(self):
        params = {
            "ds": "dataset"
        }
        path = StandardDeviationSearch.StandardDeviationSearchHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("'longitude' argument is required", body['error'])

    def test_no_latitude_400(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4"
        }
        path = StandardDeviationSearch.StandardDeviationSearchHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("'latitude' argument is required", body['error'])

    def test_no_day_or_date_400(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4",
            "latitude": "-84.32"
        }
        path = StandardDeviationSearch.StandardDeviationSearchHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("At least one of 'day' or 'date' arguments are required but not both.", body['error'])

    def test_no_day_not_int_400(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4",
            "latitude": "-84.32",
            "day": "yayday"
        }
        path = StandardDeviationSearch.StandardDeviationSearchHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("At least one of 'day' or 'date' arguments are required but not both.", body['error'])

    def test_day_and_date_400(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4",
            "latitude": "-84.32",
            "day": "35",
            "date": "1992-01-01T00:00:00Z"
        }
        path = StandardDeviationSearch.StandardDeviationSearchHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(400, response.code)
        body = json.loads(response.body)
        self.assertEqual("At least one of 'day' or 'date' arguments are required but not both.", body['error'])

    def test_no_allInTile_200(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4",
            "latitude": "-84.32",
            "day": "35"
        }
        path = StandardDeviationSearch.StandardDeviationSearchHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(200, response.code)

    def test_allInTile_200(self):
        params = {
            "ds": "dataset",
            "longitude": "22.4",
            "latitude": "-84.32",
            "date": "1992-01-01T00:00:00Z",
            "allInTile": "false"
        }
        path = StandardDeviationSearch.StandardDeviationSearchHandlerImpl.path + '?' + urllib.urlencode(params)
        response = self.fetch(path)
        self.assertEqual(200, response.code)


class TestStandardDeviationSearch(unittest.TestCase):
    def setUp(self):
        tile = Tile()
        tile.bbox = BBox(-1.0, 1.0, -2.0, 2.0)
        tile.latitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])
        tile.longitudes = np.ma.array([-2.0, -1.0, 0, 1.0, 2.0])
        tile.times = np.ma.array([0L])
        tile.data = np.ma.arange(25.0).reshape((1, 5, 5))
        tile.meta_data = {"std": np.ma.arange(25.0).reshape((1, 5, 5))}

        attrs = {'find_tile_by_polygon_and_most_recent_day_of_year.return_value': [tile]}
        self.tile_service = Mock(spec=NexusTileService, **attrs)

    def test_get_single_std_dev(self):
        print StandardDeviationSearch.get_single_std_dev(self.tile_service, "fake dataset", 1.0, .5, 83)

    def test_np(self):
        a = np.ma.array([-1.0, -0.5, 0, .5, 1.0])

        print a

        tile = self.tile_service.find_tile_by_polygon_and_most_recent_day_of_year(None, '', 2)[0]
        print tile.longitudes

        x = xrange(0)


import unittest
import numpy as np


class TestNumpyDebug(unittest.TestCase):
    def test_np(self):
        myobj = MyObject()
        myobj.longitudes = np.ma.array([-1.0, -0.5, 0, .5, 1.0])

        print myobj.longitudes
        x = 'debug'


class MyObject(object):
    def __init__(self):
        self.longitudes = None
