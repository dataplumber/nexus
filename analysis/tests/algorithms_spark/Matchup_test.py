"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import random
import timeit
import unittest
import pickle
import json

import numpy as np
from webservice.algorithms_spark.Matchup import DomsPoint, match_points, match_points_generator
from itertools import groupby


class TestMatch_Points(unittest.TestCase):
    def test_one_point_match_exact(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        matchup = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=2)

        primary_points = [primary]
        matchup_points = [matchup]

        matches = list(match_points_generator(primary_points, matchup_points, 0))

        self.assertEquals(1, len(matches))

        p_match_point, match = matches[0]

        self.assertEqual(primary, p_match_point)
        self.assertEqual(matchup, match)

    def test_one_point_match_within_tolerance_150km(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        matchup = DomsPoint(longitude=1.0, latitude=3.0, time=1000, depth=5.0, data_id=2)

        primary_points = [primary]
        matchup_points = [matchup]

        matches = list(match_points_generator(primary_points, matchup_points, 150000))  # tolerance 150 km

        self.assertEquals(1, len(matches))

        p_match_point, match = matches[0]

        self.assertEqual(primary, p_match_point)
        self.assertEqual(matchup, match)

    def test_one_point_match_within_tolerance_200m(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        matchup = DomsPoint(longitude=1.001, latitude=2.0, time=1000, depth=5.0, data_id=2)

        primary_points = [primary]
        matchup_points = [matchup]

        matches = list(match_points_generator(primary_points, matchup_points, 200))  # tolerance 200 m

        self.assertEquals(1, len(matches))

        p_match_point, match = matches[0]

        self.assertEqual(primary, p_match_point)
        self.assertEqual(matchup, match)

    def test_one_point_not_match_tolerance_150km(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        matchup = DomsPoint(longitude=1.0, latitude=4.0, time=1000, depth=5.0, data_id=2)

        primary_points = [primary]
        matchup_points = [matchup]

        matches = list(match_points_generator(primary_points, matchup_points, 150000))  # tolerance 150 km

        self.assertEquals(0, len(matches))

    def test_one_point_not_match_tolerance_100m(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        matchup = DomsPoint(longitude=1.001, latitude=2.0, time=1000, depth=5.0, data_id=2)

        primary_points = [primary]
        matchup_points = [matchup]

        matches = list(match_points_generator(primary_points, matchup_points, 100))  # tolerance 100 m

        self.assertEquals(0, len(matches))

    def test_multiple_point_match(self):
        primary = DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1)
        primary_points = [primary]

        matchup_points = [
            DomsPoint(longitude=1.0, latitude=3.0, time=1000, depth=10.0, data_id=2),
            DomsPoint(longitude=2.0, latitude=2.0, time=1000, depth=0.0, data_id=3),
            DomsPoint(longitude=0.5, latitude=1.5, time=1000, depth=3.0, data_id=4)
        ]

        matches = list(match_points_generator(primary_points, matchup_points, 150000))  # tolerance 150 km

        self.assertEquals(3, len(matches))

        self.assertSetEqual({primary}, {x[0] for x in matches})

        list_of_matches = [x[1] for x in matches]

        self.assertEquals(3, len(list_of_matches))
        self.assertItemsEqual(matchup_points, list_of_matches)

    def test_multiple_point_match_multiple_times(self):
        primary_points = [
            DomsPoint(longitude=1.0, latitude=2.0, time=1000, depth=5.0, data_id=1),
            DomsPoint(longitude=1.5, latitude=1.5, time=1000, depth=5.0, data_id=2)
        ]

        matchup_points = [
            DomsPoint(longitude=1.0, latitude=3.0, time=1000, depth=10.0, data_id=3),
            DomsPoint(longitude=2.0, latitude=2.0, time=1000, depth=0.0, data_id=4),
            DomsPoint(longitude=0.5, latitude=1.5, time=1000, depth=3.0, data_id=5)
        ]

        matches = list(match_points_generator(primary_points, matchup_points, 150000))  # tolerance 150 km

        self.assertEquals(5, len(matches))

        self.assertSetEqual({p for p in primary_points}, {x[0] for x in matches})

        # First primary point matches all 3 secondary
        self.assertEquals(3, [x[0] for x in matches].count(primary_points[0]))
        self.assertItemsEqual(matchup_points, [x[1] for x in matches if x[0] == primary_points[0]])

        # Second primary point matches only last 2 secondary
        self.assertEquals(2, [x[0] for x in matches].count(primary_points[1]))
        self.assertItemsEqual(matchup_points[1:], [x[1] for x in matches if x[0] == primary_points[1]])

    @unittest.skip("This test is just for timing, doesn't actually assert anything.")
    def test_time_many_primary_many_matchup(self):

        import logging
        import sys
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
        log = logging.getLogger(__name__)
        # Generate 160000 DomsPoints distributed equally in a box from -2.0 lat/lon to 2.0 lat/lon
        log.info("Generating primary points")
        x = np.arange(-2.0, 2.0, 0.01)
        y = np.arange(-2.0, 2.0, 0.01)
        primary_points = [DomsPoint(longitude=xy[0], latitude=xy[1], time=1000, depth=5.0, data_id=i) for i, xy in
                          enumerate(np.array(np.meshgrid(x, y)).T.reshape(-1, 2))]

        # Generate 2000 DomsPoints distributed randomly in a box from -2.0 lat/lon to 2.0 lat/lon
        log.info("Generating matchup points")
        matchup_points = [
            DomsPoint(longitude=random.uniform(-2.0, 2.0), latitude=random.uniform(-2.0, 2.0), time=1000, depth=5.0,
                      data_id=i) for i in xrange(0, 2000)]

        log.info("Starting matchup")
        log.info("Best of repeat(3, 2) matchups: %s seconds" % min(
            timeit.repeat(lambda: list(match_points_generator(primary_points, matchup_points, 1500)), repeat=3, number=2)))


class TestDOMSPoint(unittest.TestCase):
    def test_is_pickleable(self):
        edge_point = json.loads("""{
"id": "argo-profiles-5903995(46, 0)",
"time": "2012-10-15T14:24:04Z",
"point": "-33.467 29.728",
"sea_water_temperature": 24.5629997253,
"sea_water_temperature_depth": 2.9796258642,
"wind_speed": null,
"sea_water_salinity": null,
"sea_water_salinity_depth": null,
"platform": 4,
"device": 3,
"fileurl": "ftp://podaac-ftp.jpl.nasa.gov/allData/insitu/L2/spurs1/argo/argo-profiles-5903995.nc"
}""")
        point = DomsPoint.from_edge_point(edge_point, data_id=frozenset(edge_point.items()))
        self.assertIsNotNone(pickle.dumps(point))
