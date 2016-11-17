"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import importlib
import unittest
from os import environ, path

import nexusproto.NexusContent_pb2 as nexusproto
import numpy as np
from nexusproto.serialization import from_shaped_array


class TestAscatbUData(unittest.TestCase):
    def setUp(self):
        environ['INBOUND_PORT'] = '7890'
        environ['OUTBOUND_PORT'] = '7891'

        self.module = importlib.import_module('nexusxd.subtract180longitude')
        reload(self.module)

    def tearDown(self):
        del environ['INBOUND_PORT']
        del environ['OUTBOUND_PORT']

    def test_subtraction_longitudes_less_than_180(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascatb_nonempty_nexustile.bin')

        with open(test_file, 'r') as f:
            nexustile_str = f.read()

        nexus_tile_before = nexusproto.NexusTile.FromString(nexustile_str)
        longitudes_before = from_shaped_array(nexus_tile_before.tile.swath_tile.longitude)

        results = list(self.module.transform(None, nexustile_str))

        self.assertEquals(1, len(results))

        nexus_tile_after = nexusproto.NexusTile.FromString(results[0])
        longitudes_after = from_shaped_array(nexus_tile_after.tile.swath_tile.longitude)

        self.assertTrue(np.all(np.equal(longitudes_before, longitudes_after)))

    def test_subtraction_longitudes_greater_than_180(self):
        test_file = path.join(path.dirname(__file__), 'dumped_nexustiles', 'ascat_longitude_more_than_180.bin')

        with open(test_file, 'r') as f:
            nexustile_str = f.read()

        nexus_tile_before = nexusproto.NexusTile.FromString(nexustile_str)
        longitudes_before = from_shaped_array(nexus_tile_before.tile.swath_tile.longitude)

        results = list(self.module.transform(None, nexustile_str))

        self.assertEquals(1, len(results))

        nexus_tile_after = nexusproto.NexusTile.FromString(results[0])
        longitudes_after = from_shaped_array(nexus_tile_after.tile.swath_tile.longitude)

        self.assertTrue(np.all(np.not_equal(longitudes_before, longitudes_after)))
        self.assertTrue(np.all(longitudes_after[longitudes_after < 0]))
        self.assertAlmostEqual(-96.61, longitudes_after[0][26], places=2)
