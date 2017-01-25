"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from webservice.NexusHandler import NexusHandler, nexus_handler, DEFAULT_PARAMETERS_SPEC
from webservice.webmodel import NexusResults, NexusProcessingException
from datetime import datetime

@nexus_handler
class DataInBoundsSearchHandlerImpl(NexusHandler):
    name = "Data In-Bounds Search"
    path = "/datainbounds"
    description = "Fetches point values for a given dataset and geographical area"
    params = DEFAULT_PARAMETERS_SPEC
    singleton = True

    def __init__(self):
        NexusHandler.__init__(self)

    def calc(self, computeOptions, **args):
        parameter = computeOptions.get_argument('parameter', 'sst')
        if parameter not in ['sst', 'sss', 'wind']:
            raise NexusProcessingException(
                reason="Parameter %s not supported. Must be one of 'sst', 'sss', 'wind'." % parameter_s, code=400)

        min_lat = computeOptions.get_min_lat()
        max_lat = computeOptions.get_max_lat()
        min_lon = computeOptions.get_min_lon()
        max_lon = computeOptions.get_max_lon()
        ds = computeOptions.get_dataset()[0]
        start_time = computeOptions.get_start_time()
        end_time = computeOptions.get_end_time()
        includemeta = computeOptions.get_include_meta()

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
            "lon",
            "lat",
            "time"
        ]

        for i, result in enumerate(self.results()):
            cols = []

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
