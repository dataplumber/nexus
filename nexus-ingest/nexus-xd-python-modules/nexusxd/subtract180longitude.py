"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from springxd.tcpstream import start_server, LengthHeaderTcpProcessor

import nexusproto.NexusContent_pb2 as nexusproto
from nexusproto.serialization import from_shaped_array, to_shaped_array


def transform(self, tile_data):
    nexus_tile = nexusproto.NexusTile.FromString(tile_data)

    the_tile_type = nexus_tile.tile.WhichOneof("tile_type")

    the_tile_data = getattr(nexus_tile.tile, the_tile_type)

    longitudes = from_shaped_array(the_tile_data.longitude) - 180

    the_tile_data.longitude.CopyFrom(to_shaped_array(longitudes))

    yield nexus_tile.SerializeToString()


def start():
    start_server(transform, LengthHeaderTcpProcessor)


if __name__ == "__main__":
    start()
