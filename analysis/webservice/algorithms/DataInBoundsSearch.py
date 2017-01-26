"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import logging
from datetime import datetime
from pytz import timezone

from webservice.NexusHandler import NexusHandler, nexus_handler, DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))

@nexus_handler
class DataInBoundsSearchHandlerImpl(NexusHandler):
    name = "Data In-Bounds Search"
    path = "/datainbounds"
    description = "Fetches point values for a given dataset and geographical area"
    params = {
        "ds": {
            "name": "Dataset",
            "type": "string",
            "description": "The Dataset shortname to use in calculation. Required"
        },
        "parameter": {
            "name": "Parameter",
            "type": "string",
            "description": "The parameter of interest. One of 'sst', 'sss', 'wind'. Required"
        },
        "b": {
            "name": "Bounding box",
            "type": "comma-delimited float",
            "description": "Minimum (Western) Longitude, Minimum (Southern) Latitude, "
                           "Maximum (Eastern) Longitude, Maximum (Northern) Latitude. Required"
        },
        "startTime": {
            "name": "Start Time",
            "type": "string",
            "description": "Starting time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        },
        "endTime": {
            "name": "End Time",
            "type": "string",
            "description": "Ending time in format YYYY-MM-DDTHH:mm:ssZ or seconds since EPOCH. Required"
        }
    }
    singleton = True

    def __init__(self):
        NexusHandler.__init__(self)
        self.log = logging.getLogger(__name__)


    def parse_arguments(self, request):
        # Parse input arguments
        self.log.debug("Parsing arguments")

        try:
            ds = request.get_dataset()[0]
        except:
            raise NexusProcessingException(reason="'ds' argument is required", code=400)

        parameter_s = request.get_argument('parameter', None)
        if parameter_s not in ['sst', 'sss', 'wind', None]:
            raise NexusProcessingException(
                reason="Parameter %s not supported. Must be one of 'sst', 'sss', 'wind'." % parameter_s, code=400)

        try:
            start_time = request.get_start_datetime()
            start_time = long((start_time - EPOCH).total_seconds())
        except:
            raise NexusProcessingException(
                reason="'startTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)
        try:
            end_time = request.get_end_datetime()
            end_time = long((end_time - EPOCH).total_seconds())
        except:
            raise NexusProcessingException(
                reason="'endTime' argument is required. Can be int value seconds from epoch or string format YYYY-MM-DDTHH:mm:ssZ",
                code=400)

        try:
            bounding_polygon = request.get_bounding_polygon()
        except:
            raise NexusProcessingException(
                reason="'b' argument is required. Must be comma-delimited float formatted as Minimum (Western) Longitude, Minimum (Southern) Latitude, Maximum (Eastern) Longitude, Maximum (Northern) Latitude",
                code=400)

        return ds, parameter_s, start_time, end_time, bounding_polygon

    def calc(self, computeOptions, **args):
        ds, parameter, start_time, end_time, bounding_polygon = self.parse_arguments(computeOptions)

        includemeta = computeOptions.get_include_meta()

        min_lat = bounding_polygon.bounds[1]
        max_lat = bounding_polygon.bounds[3]
        min_lon = bounding_polygon.bounds[0]
        max_lon = bounding_polygon.bounds[2]

        tiles = self._tile_service.get_tiles_bounded_by_box(min_lat, max_lat, min_lon, max_lon, ds, start_time,
                                                            end_time)

        data = []
        for tile in tiles:
            for nexus_point in tile.nexus_point_generator():

                point = dict()
                point['id'] = tile.tile_id

                if parameter == 'sst':
                    point['sst'] = nexus_point.data_val.item()
                elif parameter == 'sss':
                    point['sss'] = nexus_point.data_val.item()
                elif parameter == 'wind':
                    point['wind_u'] = nexus_point.data_val.item()
                    try:
                        point['wind_v']= tile.meta_data['wind_v'][tuple(nexus_point.index)].item()
                    except (KeyError, IndexError):
                        pass
                    try:
                        point['wind_direction'] = tile.meta_data['wind_dir'][tuple(nexus_point.index)].item()
                    except (KeyError, IndexError):
                        pass
                    try:
                        point['wind_speed'] = tile.meta_data['wind_speed'][tuple(nexus_point.index)].item()
                    except (KeyError, IndexError):
                        pass
                else:
                    pass

                data.append({
                    'latitude': nexus_point.latitude,
                    'longitude': nexus_point.longitude,
                    'time': nexus_point.time,
                    'data': [
                        point
                    ]
                })

        if includemeta and len(tiles) > 0:
            meta = [tile.get_summary() for tile in tiles]
        else:
            meta = None

        result = DataInBoundsResult(
            results=data,
            stats={},
            meta=meta)

        result.extendMeta(min_lat, max_lat, min_lon, max_lon, "", start_time, end_time)

        return result

class DataInBoundsResult(NexusResults):
    def toCSV(self):
        rows = []

        headers = [
            "id",
            "lon",
            "lat",
            "time"
        ]

        for i, result in enumerate(self.results()):
            cols = []

            cols.append(str(result['data'][0]['id']))
            cols.append(str(result['latitude']))
            cols.append(str(result['longitude']))
            cols.append(datetime.utcfromtimestamp(result["time"]).strftime('%Y-%m-%dT%H:%M:%SZ'))
            if 'sst' in result['data'][0]:
                cols.append(str(result['data'][0]['sst']))
                if i == 0:
                    headers.append("sea_water_temperature")
            elif 'sss' in result['data'][0]:
                cols.append(str(result['data'][0]['sss']))
                if i == 0:
                    headers.append("sea_water_salinity")
            elif 'wind_u' in result['data'][0]:
                cols.append(str(result['data'][0]['wind_u']))
                cols.append(str(result['data'][0]['wind_v']))
                cols.append(str(result['data'][0]['wind_direction']))
                cols.append(str(result['data'][0]['wind_speed']))
                if i == 0:
                    headers.append("eastward_wind")
                    headers.append("northward_wind")
                    headers.append("wind_direction")
                    headers.append("wind_speed")

            if i == 0:
                rows.append(",".join(headers))
            rows.append(",".join(cols))

        return "\r\n".join(rows)
