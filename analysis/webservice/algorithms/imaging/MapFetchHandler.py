import StringIO
import csv
import json
from datetime import datetime

import numpy as np

from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.NexusHandler import nexus_handler
from webservice.webmodel import NexusResults

from nexustiles.model.nexusmodel import get_approximate_value_for_lat_lon

import io
from PIL import Image

@nexus_handler
class MapFetchHandler(BaseHandler):
    name = "MapFetchHandler"
    path = "/map"
    description = "Creates a map image"
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("ds", None)
        dataTimeStart = 1480492800.0 - 86400.0#computeOptions.get_datetime_arg("t", None)
        dataTimeEnd = 1480492800.0
        daysinrange = self._tile_service.find_days_in_range_asc(-90.0, 90.0, -180.0, 180.0, ds, dataTimeStart, dataTimeEnd)

        ds1_nexus_tiles = self._tile_service.get_tiles_bounded_by_box_at_time(-90.0, 90.0, -180.0, 180.0,
                                                                               ds,
                                                                               daysinrange[0])

        # Probably won't allow for user-specified dimensions. Probably.
        width = 1024
        height = 512

        img = Image.new("RGB", (width, height), "white")
        data = img.getdata()

        data_min = 100000
        data_max = -1000000
        for tile in ds1_nexus_tiles:
            data_min = np.min((data_min, np.ma.min(tile.data)))
            data_max = np.max((data_max, np.ma.max(tile.data)))

        """
            TODO: Do useful map creation stuff here
        """


        imgByteArr = io.BytesIO()
        img.save(imgByteArr, format='PNG')
        imgByteArr = imgByteArr.getvalue()

        class SimpleResult(object):
            def toJson(self):
                return json.dumps({"status": "Please specify output type as PNG."})

            def toImage(self):
                return imgByteArr

        return SimpleResult()
