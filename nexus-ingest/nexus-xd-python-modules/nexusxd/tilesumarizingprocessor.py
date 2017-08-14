"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import os
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor

import nexusproto.NexusContent_pb2 as nexusproto
import numpy
from nexusproto.serialization import from_shaped_array

try:
    var_name = os.environ["STORED_VAR_NAME"]
except KeyError:
    # STORED_VAR_NAME is optional
    pass


class NoTimeException(Exception):
    pass


def parse_input(nexus_tile_data):
    return nexusproto.NexusTile.FromString(nexus_tile_data)


def summarize_nexustile(self, tiledata):
    nexus_tile = parse_input(tiledata)
    the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

    the_tile_data = getattr(nexus_tile.tile, the_tile_type)

    latitudes = numpy.ma.masked_invalid(from_shaped_array(the_tile_data.latitude))
    longitudes = numpy.ma.masked_invalid(from_shaped_array(the_tile_data.longitude))

    data = from_shaped_array(the_tile_data.variable_data)

    if nexus_tile.HasField("summary"):
        tilesummary = nexus_tile.summary
    else:
        tilesummary = nexusproto.TileSummary()

    tilesummary.bbox.lat_min = numpy.nanmin(latitudes).item()
    tilesummary.bbox.lat_max = numpy.nanmax(latitudes).item()
    tilesummary.bbox.lon_min = numpy.nanmin(longitudes).item()
    tilesummary.bbox.lon_max = numpy.nanmax(longitudes).item()

    tilesummary.stats.min = numpy.nanmin(data).item()
    tilesummary.stats.max = numpy.nanmax(data).item()
    tilesummary.stats.mean = numpy.nanmean(data).item()
    tilesummary.stats.count = data.size - numpy.count_nonzero(numpy.isnan(data))

    try:
        min_time, max_time = find_time_min_max(the_tile_data)
        tilesummary.stats.min_time = min_time
        tilesummary.stats.max_time = max_time
    except NoTimeException:
        pass

    try:
        tilesummary.data_var_name = var_name
    except NameError:
        pass

    nexus_tile.summary.CopyFrom(tilesummary)
    yield nexus_tile.SerializeToString()


def find_time_min_max(tile_data):
    # Only try to grab min/max time if it exists as a ShapedArray
    if tile_data.HasField("time") and isinstance(tile_data.time, nexusproto.ShapedArray):
        time_data = from_shaped_array(tile_data.time)
        min_time = long(numpy.nanmin(time_data).item())
        max_time = long(numpy.nanmax(time_data).item())

        return min_time, max_time
    elif tile_data.HasField("time") and isinstance(tile_data.time, (int, long)):
        return tile_data.time, tile_data.time

    raise NoTimeException


def start():
    start_server(summarize_nexustile, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
