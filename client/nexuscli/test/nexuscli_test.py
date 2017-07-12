"""
Copyright (c) 2017 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import unittest
from datetime import datetime

from shapely.geometry import box

import nexuscli


class TestCli(unittest.TestCase):
    def test_time_series(self):
        ts = nexuscli.time_series(("AVHRR_OI_L4_GHRSST_NCEI", "AVHRR_OI_L4_GHRSST_NCEI"), box(-150, -5, -90, 5),
                                  datetime(2017, 1, 1), datetime(2017, 12, 31), spark=True)

        self.assertEqual(2, len(ts))
