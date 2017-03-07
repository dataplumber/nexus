
import json
from datetime import datetime
import time
import colortables
import numpy as np
import math
from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.NexusHandler import nexus_handler

from nexustiles.model.nexusmodel import get_approximate_value_for_lat_lon

import io
from PIL import Image

@nexus_handler
class MapFetchHandler(BaseHandler):
    name = "MapFetchHandler"
    path = "/map"
    description = "Creates a map image"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "A supported dataset shortname identifier"
        },
        "t": {
            "name": "Time",
            "type": "int",
            "description": "Data observation date"
        },
        "output": {
            "name": "Output Format",
            "type": "string",
            "description": "Output format. Use 'PNG' for this endpoint"
        },
        "min": {
            "name": "Minimum Value",
            "type": "float",
            "description": "Minimum value to use when computing color scales"
        },
        "max": {
            "name": "Maximum Value",
            "type": "float",
            "description": "Maximum value to use when computing color scales"
        },
        "ct": {
            "name": "Color Table",
            "type": "string",
            "description": "Identifier of a supported color table"
        },
        "interp": {
            "name": "Interpolation filter",
            "type": "string",
            "description": "Interpolation filter to use when rescaling image data. Can be 'nearest', 'lanczos', 'bilinear', or 'bicubic'."
        }
    }
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def __tile_to_image(self, tile, min, max, table=colortables.grayscale):
        width = len(tile.longitudes)
        height = len(tile.latitudes)
        img = Image.new("RGBA", (width, height), "white")
        img_data = img.getdata()

        for y in range(0, height):
            for x in range(0, width):
                value = tile.data[0][y][x]
                if not np.isnan(value):
                    value = np.max((min, value))
                    value = np.min((max, value))
                    value255 = int(round((value - min) / (max - min) * 255.0))
                    rgba = self.__get_color(value255, table)
                    img_data.putpixel((x, height - y - 1), (rgba[0], rgba[1], rgba[2], 255))
                else:
                    img_data.putpixel((x, height - y - 1), (0, 0, 0, 0))

        return img

    def __translate_interpolation(self, interp):
        if interp.upper() == "LANCZOS":
            return Image.LANCZOS
        elif interp.upper() == "BILINEAR":
            return Image.BILINEAR
        elif interp.upper() == "BICUBIC":
            return Image.BICUBIC
        else:
            return Image.NEAREST

    def __create_global(self, nexus_tiles, width=2048, height=1024, force_min=np.nan, force_max=np.nan, table=colortables.grayscale, interpolation="nearest"):
        img = Image.new("RGBA", (width, height), (0, 0, 0, 0))

        data_min = 100000 if np.isnan(force_min) else force_min
        data_max = -1000000 if np.isnan(force_max) else force_max

        if np.isnan(force_min) or np.isnan(force_max):
            for tile in nexus_tiles:
                if np.isnan(force_min):
                    data_min = np.min((data_min, np.ma.min(tile.data)))
                if np.isnan(force_max):
                    data_max = np.max((data_max, np.ma.max(tile.data)))

        for tile in nexus_tiles:
            tile_img = self.__tile_to_image(tile, data_min, data_max, table)

            paste_y0 = int(round(((1.0 - (tile.bbox.max_lat + 90.0) / 180.0) * height)))
            paste_x0 = int(round(((tile.bbox.min_lon + 180.0) / 360.0) * width))

            x_res = (tile.bbox.max_lon - tile.bbox.min_lon) / len(tile.longitudes)
            y_res = (tile.bbox.max_lat - tile.bbox.min_lat) / len(tile.latitudes)

            paste_y1 = int(round(((1.0 - (tile.bbox.min_lat - y_res + 90.0) / 180.0) * height)))
            paste_x1 = int(round(((tile.bbox.max_lon + x_res + 180.0) / 360.0) * width))

            tile_img = tile_img.resize((paste_x1 - paste_x0, paste_y1 - paste_y0), self.__translate_interpolation(interpolation))
            img.paste(tile_img, (paste_x0, paste_y0, paste_x1, paste_y1))

        return img

    def __get_color(self, value, table):
        index = (float(value) / float(255)) * (len(table) - 1)
        prev = int(math.floor(index))
        next = int(math.ceil(index))

        f = index - prev
        prevColor = table[prev]
        nextColor = table[next]

        r = int(round(nextColor[0] * f + (prevColor[0] * (1.0 - f))))
        g = int(round(nextColor[1] * f + (prevColor[1] * (1.0 - f))))
        b = int(round(nextColor[2] * f + (prevColor[2] * (1.0 - f))))

        return (r, g, b, 255)

    def __colorize(self, img, table):
        data = img.getdata()

        for x in range(0, img.width):
            for y in range(0, img.height):
                if data[x + (y * img.width)][3] == 255:
                    value = data[x + (y * img.width)][0]
                    rgba = self.__get_color(value, table)
                    data.putpixel((x, y), (rgba[0], rgba[1], rgba[2], 255))



    def calc(self, computeOptions, **args):
        ds = computeOptions.get_argument("ds", None)

        dataTimeEnd = computeOptions.get_datetime_arg("t", None)
        if dataTimeEnd is None:
            raise Exception("Missing 't' option for time")

        dataTimeEnd = time.mktime(dataTimeEnd.timetuple())
        dataTimeStart = dataTimeEnd - 86400.0

        color_table_name = computeOptions.get_argument("ct", "smap")
        color_table = colortables.__dict__[color_table_name]

        interpolation = computeOptions.get_argument("interp", "nearest")

        daysinrange = self._tile_service.find_days_in_range_asc(-90.0, 90.0, -180.0, 180.0, ds, dataTimeStart, dataTimeEnd)

        ds1_nexus_tiles = self._tile_service.get_tiles_bounded_by_box_at_time(-90.0, 90.0, -180.0, 180.0,
                                                                               ds,
                                                                               daysinrange[0])

        force_min = computeOptions.get_float_arg("min", np.nan)
        force_max = computeOptions.get_float_arg("max", np.nan)

        # Probably won't allow for user-specified dimensions. Probably.
        width = 4096
        height = 2048

        img = self.__create_global(ds1_nexus_tiles, width, height, force_min, force_max, color_table, interpolation)

        imgByteArr = io.BytesIO()
        img.save(imgByteArr, format='PNG')
        imgByteArr = imgByteArr.getvalue()

        class SimpleResult(object):
            def toJson(self):
                return json.dumps({"status": "Please specify output type as PNG."})

            def toImage(self):
                return imgByteArr

        return SimpleResult()
