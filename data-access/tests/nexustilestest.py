"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import pyximport
pyximport.install()

from nexus.data_access import NexusTileService

service = NexusTileService()

assert service is not None

# tiles = service.find_tiles_in_box(-90, 90, -180, 180, ds='AVHRR_OI_L4_GHRSST_NCEI')
#
# print '\n'.join([str(tile.data.shape) for tile in tiles])

#ASCATB
# tiles = service.find_tile_by_id('43c63dce-1f6e-3c09-a7b2-e0efeb3a72f2')
#MUR
# tiles = service.find_tile_by_id('d9b5afe3-bd7f-3824-ad8a-d8d3b364689c')
#SMAP
# tiles = service.find_tile_by_id('7eee40ef-4c6e-32d8-9a67-c83d4183f724')
# tile = tiles[0]
#
# print get_approximate_value_for_lat_lon([tile], np.min(tile.latitudes), np.min(tile.longitudes) + .005)
# print tile.latitudes
# print tile.longitudes
# print tile.data
# tile
# print type(tile.data)
#
# assert len(tiles) == 1
#
# tile = tiles[0]
# assert tile.meta_data is not None
#
# print tile.get_summary()
