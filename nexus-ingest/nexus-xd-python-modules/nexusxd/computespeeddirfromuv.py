"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from math import atan2, degrees, sqrt

import nexusproto.NexusContent_pb2 as nexusproto
import numpy
from nexusproto.serialization import from_shaped_array, to_shaped_array
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor
from os import environ

try:
    wind_u_var_name = environ['WIND_U']
    wind_v_var_name = environ['WIND_V']
except KeyError:
    raise RuntimeError("Both WIND_U and WIND_V environment variables are required for this script")


def calculate_speed_direction(wind_u, wind_v):
    if wind_u is numpy.ma.masked or wind_v is numpy.ma.masked:
        return numpy.ma.masked

    speed = sqrt((wind_u * wind_u) + (wind_v * wind_v))
    direction = degrees(atan2(-wind_u, -wind_v)) % 360
    return speed, direction


def transform(self, tile_data):
    nexus_tile = nexusproto.NexusTile.FromString(tile_data)

    the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

    the_tile_data = getattr(nexus_tile.tile, the_tile_type)

    # Either wind_u or wind_v are in meta. Whichever is not in meta is in variable_data
    try:
        wind_v = next(meta for meta in the_tile_data.meta_data if meta.name == wind_v_var_name).meta_data
        wind_u = the_tile_data.variable_data
    except StopIteration:
        try:
            wind_u = next(meta for meta in the_tile_data.meta_data if meta.name == wind_u_var_name).meta_data
            wind_v = the_tile_data.variable_data
        except StopIteration:
            if hasattr(nexus_tile, "summary"):
                raise RuntimeError(
                    "Neither wind_u nor wind_v were found in the meta data for granule %s slice %s."
                    " Cannot compute wind speed or direction." % (
                        getattr(nexus_tile.summary, "granule", "unknown"),
                        getattr(nexus_tile.summary, "section_spec", "unknown")))
            else:
                raise RuntimeError(
                    "Neither wind_u nor wind_v were found in the meta data. Cannot compute wind speed or direction.")

    wind_u = from_shaped_array(wind_u)
    wind_v = from_shaped_array(wind_v)

    assert wind_u.shape == wind_v.shape

    wind_speed_data = numpy.ma.empty(wind_u.shape, dtype=float)
    wind_dir_data = numpy.ma.empty(wind_u.shape, dtype=float)
    wind_u_iter = numpy.nditer(wind_u, flags=['multi_index'])
    while not wind_u_iter.finished:
        u = wind_u_iter[0]
        current_index = wind_u_iter.multi_index
        v = wind_v[current_index]

        # Calculate speed and direction values at the current index
        wind_speed, wind_dir = calculate_speed_direction(u, v)
        wind_speed_data[current_index] = wind_speed
        wind_dir_data[current_index] = wind_dir

        wind_u_iter.iternext()

    # Add wind_speed to meta data
    wind_speed_meta = the_tile_data.meta_data.add()
    wind_speed_meta.name = 'wind_speed'
    wind_speed_meta.meta_data.CopyFrom(to_shaped_array(wind_speed_data))

    # Add wind_dir to meta data
    wind_dir_meta = the_tile_data.meta_data.add()
    wind_dir_meta.name = 'wind_dir'
    wind_dir_meta.meta_data.CopyFrom(to_shaped_array(wind_dir_data))

    yield nexus_tile.SerializeToString()


def start():
    start_server(transform, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
