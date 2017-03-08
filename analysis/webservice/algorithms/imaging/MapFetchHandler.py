
import json
import time
import colortables
import numpy as np
import math
from webservice.NexusHandler import NexusHandler as BaseHandler
from webservice.NexusHandler import nexus_handler

import io
from PIL import Image
from PIL import ImageFont
from PIL import ImageDraw

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
        },
        "width": {
            "name": "Width",
            "type": "int",
            "description": "Output image width (max: 8192)"
        },
        "height": {
            "name": "Height",
            "type": "int",
            "description": "Output image height (max: 8192)"
        }
    }
    singleton = True

    NO_DATA_IMAGE = None

    def __init__(self):
        BaseHandler.__init__(self)

    @staticmethod
    def __tile_to_image(img_data, tile, min, max, table, x_res, y_res):
        width = len(tile.longitudes)
        height = len(tile.latitudes)

        d = np.ma.filled(tile.data[0], np.nan)

        for y in range(0, height):
            for x in range(0, width):
                value = d[y][x]
                if not np.isnan(value) and value != -99999:

                    lat = tile.latitudes[y]
                    lon = tile.longitudes[x]

                    pixel_y = int(math.floor(180.0 - ((lat + 90.0) * y_res)))
                    pixel_x = int(math.floor((lon + 180.0) * x_res))

                    value = np.max((min, value))
                    value = np.min((max, value))
                    value255 = int(round((value - min) / (max - min) * 255.0))
                    rgba = MapFetchHandler.__get_color(value255, table)
                    img_data.putpixel((pixel_x, pixel_y), (rgba[0], rgba[1], rgba[2], 255))

    @staticmethod
    def __translate_interpolation(interp):
        if interp.upper() == "LANCZOS":
            return Image.LANCZOS
        elif interp.upper() == "BILINEAR":
            return Image.BILINEAR
        elif interp.upper() == "BICUBIC":
            return Image.BICUBIC
        else:
            return Image.NEAREST

    @staticmethod
    def __make_tile_img(tile):
        width = len(tile.longitudes)
        height = len(tile.latitudes)
        img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        return img

    @staticmethod
    def __get_xy_resolution(tile):
        x_res = abs(tile.longitudes[0] - tile.longitudes[1])
        y_res = abs(tile.latitudes[0] - tile.latitudes[1])
        return x_res, y_res

    @staticmethod
    def __create_global(nexus_tiles, stats, width=2048, height=1024, force_min=np.nan, force_max=np.nan, table=colortables.grayscale, interpolation="nearest"):

        data_min = stats["minValue"] if np.isnan(force_min) else force_min
        data_max = stats["maxValue"] if np.isnan(force_max) else force_max

        x_res, y_res = MapFetchHandler.__get_xy_resolution(nexus_tiles[0])

        canvas_width = int(360.0 / x_res)
        canvas_height = int(180.0 / y_res)
        img = Image.new("RGBA", (canvas_width, canvas_height), (0, 0, 0, 0))
        img_data = img.getdata()

        for tile in nexus_tiles:
            MapFetchHandler.__tile_to_image(img_data, tile, data_min, data_max, table, x_res, y_res)

        final_image = img.resize((width, height), MapFetchHandler.__translate_interpolation(interpolation))

        return final_image

    @staticmethod
    def __get_color(value, table):
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

    @staticmethod
    def __colorize(img, table):
        data = img.getdata()

        for x in range(0, img.width):
            for y in range(0, img.height):
                if data[x + (y * img.width)][3] == 255:
                    value = data[x + (y * img.width)][0]
                    rgba = MapFetchHandler.__get_color(value, table)
                    data.putpixel((x, y), (rgba[0], rgba[1], rgba[2], 255))

    @staticmethod
    def __create_no_data(width, height):

        if MapFetchHandler.NO_DATA_IMAGE is None:
            img = Image.new("RGBA", (width, height), (0, 0, 0, 0))
            draw = ImageDraw.Draw(img)

            fnt = ImageFont.truetype('webservice/algorithms/imaging/Roboto/Roboto-Bold.ttf', 40)

            for x in range(0, width, 500):
                for y in range(0, height, 500):
                    draw.text((x, y), "NO DATA", (180, 180, 180), font=fnt)
            MapFetchHandler.NO_DATA_IMAGE = img

        return MapFetchHandler.NO_DATA_IMAGE

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

        force_min = computeOptions.get_float_arg("min", np.nan)
        force_max = computeOptions.get_float_arg("max", np.nan)

        width = np.min([8192, computeOptions.get_int_arg("width", 1024)])
        height = np.min([8192, computeOptions.get_int_arg("height", 512)])

        stats = self._tile_service.get_dataset_overall_stats(ds)

        daysinrange = self._tile_service.find_days_in_range_asc(-90.0, 90.0, -180.0, 180.0, ds, dataTimeStart, dataTimeEnd)

        if len(daysinrange) > 0:
            ds1_nexus_tiles = self._tile_service.get_tiles_bounded_by_box_at_time(-90.0, 90.0, -180.0, 180.0,
                                                                                   ds,
                                                                                   daysinrange[0])

            img = self.__create_global(ds1_nexus_tiles, stats, width, height, force_min, force_max, color_table, interpolation)
        else:
            img = self.__create_no_data(width, height)

        imgByteArr = io.BytesIO()
        img.save(imgByteArr, format='PNG')
        imgByteArr = imgByteArr.getvalue()

        class SimpleResult(object):
            def toJson(self):
                return json.dumps({"status": "Please specify output type as PNG."})

            def toImage(self):
                return imgByteArr

        return SimpleResult()
